"""TOU Scheduler for Home Assistant."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
import logging
from zoneinfo import ZoneInfo

import pandas as pd

from homeassistant.components import mqtt
from homeassistant.components.recorder import get_instance, statistics
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store

from .const import (
    BATTERY_LOST_WH,
    BATTERY_MAX_WH,
    BATTERY_SOC,
    BATTERY_WH_PER_PERCENT,
    DATA_KEY,
    DEBUGGING,
    DEFAULT_GRID_BOOST_HISTORY,
    DEFAULT_GRID_BOOST_MIDNIGHT_SOC,
    DEFAULT_GRID_BOOST_START,
    DEFAULT_GRID_BOOST_STARTING_SOC,
    DEFAULT_INVERTER_EFFICIENCY,
    FORECAST_KEY,
    GRID_BOOST,
    LOAD_POWER,
    PV_POWER,
    SHADE_KEY,
)
from .coordinator import TOUUpdateCoordinator
from .solcast_api import SolcastAPI

logger = logging.getLogger(__name__)
if DEBUGGING:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


# Helper functions
def string_to_int_list(string_list) -> list[int]:
    """Convert a string containing one or more integers into a list of ints."""
    return [int(i.strip()) for i in string_list.split(",") if i.strip().isdigit()]


def printable_hour(hour: int) -> str:
    """Return a printable hour string in 12-hour format with 'am' or 'pm' suffix.

    Takes an integer and returns a formatted string in 12-hour format with am/pm.
    """
    # Check for invalid hour and just return the number as a string
    if hour < 0 or hour > 23:
        return str(hour)
    return (
        f"{'\u00a0' if ((hour%12 < 10) and hour%12 > 0) else ''}"
        f"{(hour % 12) or 12}"
        f"{'am' if hour < 12 else 'pm'}"
    )


class TOUScheduler:
    """Class to manage Time of Use (TOU) scheduling for Home Assistant.

    The class contains:
      - 1 constructor to initialize the TOU Scheduler.
      - 7 methods to manage data loading, saving, and requesting data from Home Assistant.
      - 3 methods to manage options changes by the user.
      - 4 private calculation methods.
      - 1 private method to update certain data every hour.
      - 3 public method
      - 3  public methods to start the TOU Scheduler:
        a)  Load data and run necessary functions when starting the TOU Scheduler.
        b)  Update the sensor data every 5 minutes with the latest data.
        c)  Pass the sensor dictionary data to Home Assistant.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        timezone: str,
        solcast_api: SolcastAPI,
        coordinator: TOUUpdateCoordinator | None,
    ) -> None:
        """Initialize the TOU Scheduler."""
        self.hass = hass
        self.config_entry = config_entry
        self.coordinator = coordinator
        self.timezone = timezone
        self.status = "Starting"
        self.store_shade: Store = Store(hass, version=1, key=SHADE_KEY)
        self.store_forecast: Store = Store(hass, version=1, key=FORECAST_KEY)
        self.store_data: Store = Store(hass, version=1, key=DATA_KEY)

        # Here is the inverter info
        self.load_estimates: dict[str, dict[int, float]] = {}
        self.load_estimates_updated: date | None = None
        self.daily_load_averages: dict[int, float] = {}

        # Here is the battery info (capacity is Ah, flow is A)
        self.battery_capacity: float = 0.0
        self.battery_capacity_last_updated: datetime | None = None
        self.battery_flow: float = 0.0

        # Here is the solcast info
        self.solcast_api: SolcastAPI = solcast_api

        # Here is the shading info: default to 0.0 for each hour of the day and no last update date
        self.daily_shading: dict[int, float] = dict.fromkeys(range(24), 0.0)

        # Here is the TOU boost info we will monitor and update
        self.manual_grid_boost: int = DEFAULT_GRID_BOOST_STARTING_SOC
        self.batt_minutes_remaining: int = 0
        self.calculated_grid_boost: int = DEFAULT_GRID_BOOST_STARTING_SOC
        self.calculated_grid_boost_day: str = ""
        self.min_battery_soc: int = DEFAULT_GRID_BOOST_MIDNIGHT_SOC
        self.grid_boost_start: str = DEFAULT_GRID_BOOST_START
        self._boost: str = "Testing"
        self.days_of_load_history: int = DEFAULT_GRID_BOOST_HISTORY
        self._update_tou_boost: bool = False

        # Time for hourly tasks
        self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))

    # Data loading and saving
    async def async_load_shading(self):
        """Load shading data from storage."""
        data = await self.store_shade.async_load()
        data = {int(k): v for k, v in data.items()} if data else None
        if data is not None:
            self.daily_shading = data

    async def async_save_shading(self):
        """Save shading data to storage."""
        await self.store_shade.async_save(self.daily_shading)

    async def async_load_forecast(self):
        """Load forecast data from storage."""
        data = await self.store_forecast.async_load()
        if data is not None:
            self.solcast_api.forecast = data
            # Set the update variable based on the oldest date in the data
            first_date = min(self.solcast_api.forecast.keys())
            # Get the first period_end from the forecast data and set the data_updated date to this date
            self.solcast_api.data_updated = datetime.strptime(
                first_date, "%Y-%m-%d-%H"
            ) - timedelta(hours=1)

    async def async_save_forecast(self):
        """Save forecast data to storage."""
        await self.store_forecast.async_save(self.solcast_api.forecast)

    async def async_load_data(self):
        """Load data from storage."""
        data = await self.store_data.async_load()
        if data:
            self.manual_grid_boost = data.get("manual_grid_boost", DEFAULT_GRID_BOOST_STARTING_SOC)
            self.calculated_grid_boost = data.get("calculated_grid_boost", DEFAULT_GRID_BOOST_STARTING_SOC)
            self.calculated_grid_boost_day = data.get("calculated_grid_boost_day", "")
            self.min_battery_soc = data.get("min_battery_soc", DEFAULT_GRID_BOOST_MIDNIGHT_SOC)
            self.grid_boost_start = data.get("grid_boost_start", DEFAULT_GRID_BOOST_START)
            self._boost = data.get("_boost", "Testing")
            self.days_of_load_history = data.get("days_of_load_history", DEFAULT_GRID_BOOST_HISTORY)

    async def async_save_data(self):
        """Save data to storage."""
        data = {
            "manual_grid_boost": self.manual_grid_boost,
            "calculated_grid_boost": self.calculated_grid_boost,
            "calculated_grid_boost_day": self.calculated_grid_boost_day,
            "min_battery_soc": self.min_battery_soc,
            "grid_boost_start": self.grid_boost_start,
            "_boost": self._boost,
            "days_of_load_history": self.days_of_load_history,
        }
        await self.store_data.async_save(data)

    async def async_stop(self):
        """Stop the TOU Scheduler."""
        await self.async_save_data()


    # SERVICE CALLS start here
    async def async_update_boost_settings(
        self,
        boost_mode: str,
        manual_grid_boost: int,
        min_battery_soc: int,
        percentile: int,
        days_of_load_history: int,
        boost_hour: int,
    ) -> None:
        """Service call to allow tou-scheduler-card.js to return settings data."""
        self._boost = boost_mode
        self.manual_grid_boost = manual_grid_boost
        self.min_battery_soc = min_battery_soc
        self.solcast_api.percentile = percentile
        self.days_of_load_history = days_of_load_history
        self.solcast_api.update_hour = boost_hour

        # We probably need to rerun the hourly updates
        self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))
        await self._hourly_updates()

        # Call the coordinator and request async_request_refresh
        if self.coordinator:
            await self.coordinator.async_request_refresh()

    async def set_boost(self, boost: str) -> None:
        """Set the boost mode."""
        self._boost = boost
        self.hass.config_entries.async_update_entry(
            self.config_entry,
            options={**self.config_entry.options, "boost_mode": boost},
        )
        # We need to rerun the hourly updates
        self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))
        await self._hourly_updates()

    async def set_manual_grid_boost(self, manual_grid_boost: int) -> None:
        """Set the manual grid boost value."""
        self.manual_grid_boost = manual_grid_boost
        self.hass.config_entries.async_update_entry(
            self.config_entry,
            options={
                **self.config_entry.options,
                "manual_grid_boost": manual_grid_boost,
            },
        )
        # We probably need to rerun the hourly updates
        self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))
        await self._hourly_updates()

    async def set_solcast_percentile(self, percentile: int) -> None:
        """Set the Solcast percentile value."""
        self.solcast_api.percentile = percentile
        self.hass.config_entries.async_update_entry(
            self.config_entry,
            options={**self.config_entry.options, "percentile": percentile},
        )
        # We need to rerun the hourly updates
        self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))
        await self._hourly_updates()

    async def set_solcast_update_hour(self, update_hour: str) -> None:
        """Set the Solcast update hours."""
        self.solcast_api.update_hour = int(update_hour)
        self.hass.config_entries.async_update_entry(
            self.config_entry,
            options={**self.config_entry.options, "forecast_hour": update_hour},
        )
        # We probably need to rerun the hourly updates
        self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))
        await self._hourly_updates()

    async def set_days_of_load_history(self, days_of_load_history: int) -> None:
        """Set the days of load history."""
        self.days_of_load_history = days_of_load_history
        self.hass.config_entries.async_update_entry(
            self.config_entry,
            options={**self.config_entry.options, "history_days": days_of_load_history},
        )
        # We probably need to rerun the hourly updates
        self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))
        await self._hourly_updates()

    # SERVICE CALLS end here

    # Home Assistant calls to get statistics
    async def _request_ha_statistics(
        self, entity_ids: set[str], days: int
    ) -> defaultdict:
        """Request the mean hourly statistics for the given entity_id using units unit for the number of days specified.

        This uses a background task to prevent thread blocking.
        """
        # Initialize the start and end date range for yesterday
        now = datetime.now(ZoneInfo(self.timezone))
        end_time = datetime.combine(now.date(), datetime.min.time()).replace(
            tzinfo=ZoneInfo(self.timezone)
        )
        start_time = end_time - timedelta(days=int(days))

        # Convert start_time and end_time to UTC
        start_time_utc = start_time.astimezone(ZoneInfo("UTC"))
        end_time_utc = end_time.astimezone(ZoneInfo("UTC"))

        # # Log the request
        # logger.debug(
        #     "Statistics for %s gathered from %s until %s.",
        #     entity_ids,
        #     start_time_utc.strftime("%a at %-I %p"),
        #     end_time_utc.strftime("%a at %-I %p"),
        # )

        return await get_instance(self.hass).async_add_executor_job(
            self._get_statistics_during_period,
            start_time_utc,
            end_time_utc,
            entity_ids,
        )

    async def _get_pv_statistic_for_last_hour(self) -> float:
        """Get the mean PV power for the last hour."""
        # Set the sensor entity_id and the start and end time to get the last hour's statistics
        entity_id = PV_POWER
        now = datetime.now(ZoneInfo(self.timezone))
        start_of_last_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(
            hours=1
        )
        end_of_last_hour = (
            start_of_last_hour + timedelta(hours=1) - timedelta(microseconds=1)
        )

        # Get the statistics for the last hour
        stats = await get_instance(self.hass).async_add_executor_job(
            self._get_statistics_during_period,
            start_of_last_hour,
            end_of_last_hour,
            {entity_id},
        )

        # Extract the mean value for the entity_id
        mean_value = stats.get(entity_id, [{}])[0].get("mean", 0.0)
        # logger.debug(
        #     ">>>>PV power generation at %s was %s wH<<<<",
        #     printable_hour(start_of_last_hour.hour),
        #     mean_value,
        # )
        return mean_value

    def _get_statistics_during_period(
        self,
        start_time: datetime,
        end_time: datetime,
        entity_ids: set[str],
    ) -> defaultdict:
        """Get statistics during the specified period."""
        stats = statistics.statistics_during_period(
            self.hass,
            start_time,
            end_time,
            entity_ids,
            "hour",
            None,
            {"mean"},
        )
        return defaultdict(list, stats)
    # End of Home Assistant calls to get statistics

    # START of Home Assistant calls to get/set current sensor states and number values
    async def get_sensor_state(self, entity_id: str) -> float:
        """Get the current state of this entity as a float, or 0.0 if unavailable or invalid."""
        state = self.hass.states.get(entity_id=entity_id)
        if state is None or state.state in (None, "unknown", "unavailable"):
            return 0.0
        try:
            return float(state.state)
        except (ValueError, TypeError):
            return 0.0
        
    async def write_grid_boost_soc(self, boost: int) -> None:
        """Set the grid boost value into capacity point 1."""
        if self._boost in {"Testing", "Off"}:
            return
        state = await self.get_sensor_state(entity_id=GRID_BOOST)
        if state is None or state == 0.0:
                logger.error("MQTT is not connected. Cannot set grid boost.")
                self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))
                return

        logger.debug(
            "Trying async method to set grid boost to %s%%.",
            boost,
        )
        my_topic = "sa/inverter_1/capacity_point_1/set"
        service_data = { "payload": str(boost), "topic": my_topic }
        # logger.info("Service data: %s", service_data)
        await self.hass.services.async_call("mqtt","publish", service_data, False)
        
    async def _get_battery_capacity(self) -> float:
        """Get the total battery capacity in Ah from all sa/battery_*/capacity entities once a day if > 0."""
        if self.battery_capacity_last_updated is not None:
            # If we have already updated the battery capacity today, return the cached value
            if self.battery_capacity_last_updated.date() == datetime.now(ZoneInfo(self.timezone)).date():
                return self.battery_capacity
        total_capacity = 0.0
        # Iterate over all states in Home Assistant
        for state in self.hass.states.async_all():
            if not hasattr(state, "entity_id") or not hasattr(state, "state"):
                continue
            if state.entity_id.startswith("sensor.deye_sunsynk_sol_ark_capacity"):
                try:
                    value = float(state.state)
                    total_capacity += value
                except (ValueError, TypeError):
                    continue
        # If we have a total capacity, update the battery capacity and last updated date
        if total_capacity > 0:
            self.battery_capacity = total_capacity
            self.battery_capacity_last_updated = datetime.now(ZoneInfo(self.timezone))
            logger.debug(
                "Total battery capacity updated to %.2f Ah at %s",
                self.battery_capacity,
                self.battery_capacity_last_updated.strftime("%Y-%m-%d %H:%M:%S"),
            )
        else:
            logger.warning("No valid battery capacity found. Using 0.0 Ah.")
            self.battery_capacity = 0.0
            self.battery_capacity_last_updated = None
        return total_capacity
    
    async def _get_battery_flow(self) -> float:
        """Get the total battery current flow in A from all batteries."""
        state = self.hass.states.get("sensor.deye_sunsynk_sol_ark_battery_current")
        if state is None or state.state in (None, "unknown", "unavailable"):
            return 0.0
        try:
            return float(state.state)
        except (ValueError, TypeError):
            return 0.0
        
    # End of Home Assistant calls to get current sensor states


    # Private calculation methods
    async def _calculate_shading(self) -> None:
        """Calculate the shading each hour.

        We will store the shading for each hour of the day in a dictionary. We write this
        to Home Assistant Storage so we can load past shading if we restart Home Assistant.

        Each update we record changes to actual PV power generated. We use this to compute an average PV power for the hour.
        At the beginning of a new hour, we compute the past hour average PV power and compare it to the estimated PV power and
        the estimated amount of sun. If the sun was full and the battery was charging we can adjust the shading and write
        the adjusted shading info to Home Assistant Storage, otherwise leave it alone.

        We then reset the average PV power to the first value for this hour. and repeat the process for the next hour.
        """

        # Only do this if we are in automated mode or testing mode
        if self._boost not in {"Testing", "Automated"}:
            return

        # Get the pv data for the current hour, calculate the average PV power for the past hour, updating as needed
        pv_average = await self._get_pv_statistic_for_last_hour()

        # logger.debug(
        #     "Average PV power for the past hour: %s, last sun estimate is: %.2f",
        #     pv_average,
        #     self.solcast_api.get_previous_hour_sun_estimate(),
        # )
        # Update shading if we had a positive average PV power, battery soc is low enough to allow charging, and the sun was full
        last_hour = (datetime.now(ZoneInfo(self.timezone)).hour - 1) % 24
        soc = await self.get_sensor_state(BATTERY_SOC)

        if (
            pv_average > 0
            and self.solcast_api.get_previous_hour_pv_estimate() > 0
            and soc < 96
            and self.solcast_api.get_previous_hour_sun_estimate() > 0.95
        ):
            shading = 1 - min(
                pv_average / self.solcast_api.get_previous_hour_pv_estimate(), 1
            )
            self.daily_shading[last_hour] = shading
            logger.info(
                "Shading for %s changed to %s. Last hour sun estimate is %.2f",
                last_hour,
                shading,
                self.solcast_api.get_previous_hour_sun_estimate(),
            )

            # Write the shading to the hass storage
            await self.async_save_shading()

    async def _calculate_load_estimates(self) -> None:
        """Calculate the daily load averages once a day."""

        # Skip already done today
        if (
            self.load_estimates_updated is not None
            and self.load_estimates_updated
            == datetime.now(ZoneInfo(self.timezone)).date()
        ):
            return

        load_entity_ids = {LOAD_POWER}
        history_data = await self._request_ha_statistics(
            load_entity_ids, self.days_of_load_history
        )

        data = []
        for data_list in history_data.values():
            for item in data_list:
                # Ensure that each item is valid
                if (
                    not isinstance(item, dict)
                    or "start" not in item
                    or "mean" not in item
                    or item["mean"] == 0
                ):
                    logger.debug("Skipping invalid item: %s", item)
                    continue

                start_time = datetime.fromtimestamp(
                    item["start"], tz=ZoneInfo(self.timezone)
                )
                data.append({"hour": start_time.hour, "mean": item["mean"]})

        # Create DataFrame
        df = pd.DataFrame(data)

        # Check if DataFrame is empty or missing columns
        if df.empty or "hour" not in df.columns or "mean" not in df.columns:
            logger.warning("No valid load data found. Skipping load estimates.")
            self.daily_load_averages = dict.fromkeys(range(24), 1000.0)
            return

        # Group by "hour" and get averages
        self.daily_load_averages = df.groupby("hour")["mean"].mean().to_dict()

        self.load_estimates_updated = datetime.now(ZoneInfo(self.timezone)).date()


    async def _calculate_tou_battery_remaining_time(self) -> None:
        """Calculate remaining battery life."""
        # For each hour going forward, we need to calculate how long the battery will last until completely exhausted.
        # Set initial variables
        now = datetime.now(ZoneInfo(self.timezone))
        day = now.date()
        hour: int = now.hour
        starting_time = datetime.now(ZoneInfo(self.timezone)).strftime(
            "%a %-I %p"
        )
        soc = await self.get_sensor_state(BATTERY_SOC)
        # If the battery is empty, MQTT is not connected, or the inverter is off, return
        if soc == 0:
            return
        batt_wh_usable = soc / 100 * BATTERY_MAX_WH - BATTERY_LOST_WH
        minutes = 0

        # Log for debugging
        logger.debug("------- Calculating remaining battery time -------")
        logger.debug(
            "Starting at %s with %s wH usable energy.",
            printable_hour(hour),
            batt_wh_usable,
        )
        while batt_wh_usable > 0 and minutes < 5 * 24 * 60:
            # For each hour, calculate the impact of the solar generation and load.
            load = (
                self.daily_load_averages.get(hour, 1000) / DEFAULT_INVERTER_EFFICIENCY
            )
            pv = (
                1000
                * self.solcast_api.forecast.get(f"{day}-{hour}", (0.0, 0.0))[0]
                * (1 - self.daily_shading.get(hour, 0.0))
            )
            batt_wh_usable = batt_wh_usable - load + pv
            logger.debug(
                "At %s battery energy is %6s wH.",
                printable_hour((hour + 1) % 24),
                f"{batt_wh_usable:6,.0f}",
            )
            # Monitor progress
            if batt_wh_usable > 0:
                minutes += 60
            else:
                minutes -= int((batt_wh_usable / (pv - load)) * 60)
            # Move to next hour
            hour = (hour + 1) % 24
            if hour == 0:
                day = day + timedelta(days=1)

        self.batt_minutes_remaining = minutes
        if minutes >= 5 * 24 * 60:
            logger.info(
                "At %s: Battery will not be exhausted in the next 5 days.",
                starting_time,
            )
        else:
            logger.info(
                "At %s: Estimating %.1f hours of battery life.",
                starting_time,
                minutes / 60,
            )

    async def _calculate_tou_boost_soc(self) -> None:
        """Calculate tomorrow off-peak grid boost required SoC.

        Save and log any changes
        Update the inverter with the new SoC.
        Log the results showing the impact of the changes.
        """

        # Initialize variables
        required_soc = float(DEFAULT_GRID_BOOST_STARTING_SOC)
        tomorrow = datetime.now(ZoneInfo(self.timezone)).date() + timedelta(days=1)
        self.calculated_grid_boost_day = tomorrow.strftime("%a")
        # We want to track the lowest SoC as we walk through the hours
        lowest_point = 0.0
        running_soc = 0.0

        # Do the initial calculation for the grid boost SoC
        for hour in range(6, 23):
            # Calculate the load for the hour (multiplied by the efficiency factor)
            load = (
                self.daily_load_averages.get(int(hour), 1000)
                * DEFAULT_INVERTER_EFFICIENCY
            )
            pv = (
                1000
                * self.solcast_api.forecast.get(f"{tomorrow}-{hour}", (0.0, 0.0))[0]
                * (1 - self.daily_shading.get(hour, 0.0))
            )
            net_power = pv - load
            net_soc = net_power / BATTERY_WH_PER_PERCENT
            running_soc += net_soc
            lowest_point = min(lowest_point, running_soc)
        # We want the starting SoC to be such that our lowest point doesn't drop below the midnight SoC
        lowest_point -= self.min_battery_soc
        # Calculate the required SoC for the grid boost so we don't drop below the minimum SoC
        soc = round(-lowest_point, 0)
        # Prevent required SoC from going above 99%
        required_soc = min(99, soc)
        # Save the new off-peak grid boost level
        self.calculated_grid_boost = int(round(required_soc, 0))

        # Test grid boost SoC to make sure we end up with the minimum SoC at midnight
        boost: float = self.calculated_grid_boost if self._boost == "Automatic" else self.manual_grid_boost

        # Prepare the final results nicely for the user logs
        hyphen_format = "{:-^53}"
        logger.info(
            msg=hyphen_format.format(
                f" Off-peak charging for {tomorrow.strftime("%A")} starting at {boost}% SoC "
            )
        )
        # Calculate additional SOC needed to reach midnight
        logger.info("Hour:    PV  Shade    Load   Net Power   ± SoC    SoC")

        for hour in range(6, 24):
            # Calculate the load for the hour (multiplied by the efficiency factor)
            hour_load = (
                self.daily_load_averages.get(int(hour), 1000)
                * DEFAULT_INVERTER_EFFICIENCY
            )
            hour_pv_forecast = self.solcast_api.forecast.get(f"{tomorrow}-{hour}", (0.0, 0.0))
            hour_pv = hour_pv_forecast[0] * 1000
            hour_shading = self.daily_shading.get(hour, 0.0)
            hour_net_pv = hour_pv - hour_pv * (hour_shading)
            net_power = hour_net_pv - hour_load
            hour_soc = net_power / BATTERY_WH_PER_PERCENT
            boost += hour_soc
            boost = min(100.0, boost)
            shade = int(round(hour_shading * 100, 2))
            logger.info(
                f"{printable_hour(hour)}: {hour_pv:5,.0f}  {shade:4d}%  {hour_load:6,.0f}  {net_power:7,.0f} wH   {hour_soc:4,.1f}%  {boost:4,.0f}%"  # noqa: G004
            )
        logger.info(msg=hyphen_format.format(f"Done calculating grid boost SoC. Auto = {self.calculated_grid_boost}"))

        # Write the new grid boost SoC to the inverter
        await self.write_grid_boost_soc(self.calculated_grid_boost)

    # Private hourly update method (called by hourly task)
    async def _hourly_updates(self) -> None:
        """Update the hourly data for the sensors.

        We need to update the following data:
        - Solcast forecast data
        - Daily shading data
        - Off-peak grid boost SoC
        - Remaining battery life
        """
        # First, check if this is the right time for an hourly update. Return if not.
        if self._hourly_task_next_start >= datetime.now(ZoneInfo(self.timezone)):
            return

        # Reset the next hourly update time and continue with the updates
        now = datetime.now(ZoneInfo(self.timezone))
        self._hourly_task_next_start = now.replace(minute=7, second=0, microsecond=0) + timedelta(hours=1)
        # Update the daily load estimates (actually once a day, managed the function)
        await self._calculate_load_estimates()
        # Try to update the Solcast data (true if successful at startup and as per user schedule)
        self._update_tou_boost = await self.solcast_api.refresh_data()
        # Update the hourly shading values
        await self._calculate_shading()
        # If the forecast data changed, save that data and recompute off-peak grid boost SoC
        if self._update_tou_boost:
            await self.async_save_forecast()
            self._update_tou_boost = False
        # Calculate the off-peak grid boost SoC in case the user changed other settings like days of load history
        # BUT... only do this after the current morning boost period has ended
        if datetime.now(ZoneInfo(self.timezone)).hour >= 6:
            await self._calculate_tou_boost_soc()
        # Compute remaining battery life for the user
        await self._calculate_tou_battery_remaining_time()


    # Start the TOU Scheduler - load saved data and start the background tasks
    async def async_start(self) -> None:
        """Set up the config options callback when starting the TOU Scheduler."""
        # Ensure config_entry is set
        if not self.config_entry:
            logger.error("Config entry is not set.")
            return
        # Load the data from storage
        await self.async_load_data()
        # Load the shading data from storage
        await self.async_load_shading()
        # Load the forecast data from storage
        
        await self.async_load_forecast()
        # Load the battery capacity and flow. 
        # NOTE: Can't do this yet as mqtt may not be connected quite yet.


    async def to_dict(self) -> dict[str, float | str | datetime]:
        """Return this sensor data as a dictionary.

        This method provides expected battery life statistics and the grid boost value for the upcoming day.
        It also returns the inverter_api data and the solcast_api data.

        Returns:
            dict[str, Any]: A dictionary containing the sensor data.

        """
        # Do hourly updates if needed
        await self._hourly_updates()

        # Save the state of the key data every time we update the data
        await self.async_save_data()

        # Get the current hour
        hour = datetime.now(ZoneInfo(self.timezone)).hour
        exhausted = datetime.now(tz=ZoneInfo(self.timezone)) + timedelta(
            minutes=self.batt_minutes_remaining
        )
        actual_grid_boost = await self.get_sensor_state(GRID_BOOST)
        # Calculate the battery flow in %/hr (assumes flow is constant, which is not actually true)
        self.battery_capacity = await self._get_battery_capacity()
        self.battery_flow = await self._get_battery_flow()
        batt_change_per_hour = 100 * self.battery_flow / self.battery_capacity if self.battery_capacity > 0 else 0.0
        soc = await self.get_sensor_state(BATTERY_SOC)
        batt_next_hour =  max(0,min(100,soc + batt_change_per_hour))
        logger.debug(
            "Battery SoC is %0.1f%%, change per hour is %0.1f%%, estimated in 1 hour: %0.1f%%.",
            soc,
            batt_change_per_hour,
            batt_next_hour
        )
        # logger.debug(
        #     "Battery flow is %s A, capacity is %s Ah, change per hour is %f%%.",
        #     self.battery_flow,
        #     self.battery_capacity,
        #     batt_change_per_hour,
        # )
        # Return the data as a dictionary
        return {
            # Battery data
            "batt_time": self.batt_minutes_remaining / 60,
            "batt_exhausted": exhausted,
            "batt_change_per_hour": batt_change_per_hour,
            "batt_next_hour": batt_next_hour,
            # PV data
            "power_pv_estimated": self.solcast_api.get_previous_hour_pv_estimate(),
            "forecast_today": self.solcast_api.forecast_today,
            "forecast_tomorrow": self.solcast_api.forecast_tomorrow,
            # Inverter info
            "load_estimate": self.load_estimates.get(str(hour), {}).get(hour, 1000),
            # Boost data
            "actual_grid_boost": actual_grid_boost,
            "manual_grid_boost": self.manual_grid_boost,
            "grid_boost_mode": self._boost,
            "grid_boost_soc": self.calculated_grid_boost,
            "grid_boost_day": self.calculated_grid_boost_day,
            "grid_boost_start": self.grid_boost_start,
            "grid_boost_on": self._boost,
            "update_hour": self.solcast_api.update_hour,
            # Boost entity
            "calculated_boost": self.calculated_grid_boost,
            "manual_boost": self.manual_grid_boost,
            "min_soc": self.min_battery_soc,
            "confidence": self.solcast_api.percentile,
            "load_days": self.days_of_load_history,
            # Daily data
            "shading": str(self.daily_shading),
            "load": str(self.daily_load_averages),
        }
