"""TOU Scheduler for Home Assistant using Solar Assistant with MQTT and SolArk (Deye) inverter."""

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
    BATTERY_SOC,
    BATTERY_WH_PER_PERCENT,
    FIVE_DAYS,
    DATA_KEY,
    DEBUGGING,
    DEFAULT_GRID_BOOST_HISTORY,
    DEFAULT_GRID_BOOST_MIDNIGHT_SOC,
    DEFAULT_GRID_BOOST_START,
    DEFAULT_GRID_BOOST_STARTING_SOC,
    DEFAULT_INVERTER_EFFICIENCY,
    FORECAST_KEY,
    BATTERY_CAPACITY,
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
        self.battery_flow: float = 0.0  # Current battery flow in watts
        self.battery_shutdown: int = 0  # Battery shutdown SoC
        self.battery_capacity: float = 0.0  # Battery capacity in Ah
        self._battery_capacity_last_updated: datetime | None = None  # Last time the battery capacity was updated
        self.realtime_battery_soc: float = 0.0  # Current battery state of charge in %

        # Here is the solcast info
        self.solcast_api: SolcastAPI = solcast_api

        # Here is the shading info: default to 0.0 for each hour of the day and no last update date
        self.daily_shading: dict[int, float] = dict.fromkeys(range(24), 0.0)

        # Here is the TOU boost info we will monitor and update
        self.manual_grid_boost: int = DEFAULT_GRID_BOOST_STARTING_SOC
        self.battery_minutes_remaining: int = 0
        self.calculated_grid_boost: int = DEFAULT_GRID_BOOST_STARTING_SOC
        self.calculated_grid_boost_day: str = ""
        self.actual_grid_boost: int = DEFAULT_GRID_BOOST_STARTING_SOC
        self.min_battery_soc: int = DEFAULT_GRID_BOOST_MIDNIGHT_SOC
        self.grid_boost_start: str = DEFAULT_GRID_BOOST_START
        self._boost: str = "Testing"
        self.days_of_load_history: int = DEFAULT_GRID_BOOST_HISTORY
        self._update_tou_boost: bool = False

        # Time for hourly tasks
        self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))

    # Data loading and saving. There are three data stores: shading, forecast, and general data
    async def _load_shading(self):
        """Load shading data from storage."""
        data = await self.store_shade._load()
        data = {int(k): v for k, v in data.items()} if data else None
        if data is not None:
            self.daily_shading = data

    async def _save_shading(self):
        """Save shading data to storage."""
        await self.store_shade._save(self.daily_shading)

    async def _load_forecast(self):
        """Load forecast data from storage."""
        data = await self.store_forecast._load()
        if data is not None:
            self.solcast_api.forecast = data
            # Set the update variable based on the oldest date in the data
            first_date = min(self.solcast_api.forecast.keys())
            # Get the first period_end from the forecast data and set the data_updated date to this date
            self.solcast_api.data_updated = datetime.strptime(
                first_date, "%Y-%m-%d-%H"
            ) - timedelta(hours=1)

    async def _save_forecast(self):
        """Save forecast data to storage."""
        await self.store_forecast._save(self.solcast_api.forecast)

    async def _load_data(self):
        """Load data from storage."""
        data = await self.store_data._load()
        if data:
            self.actual_grid_boost = data.get("actual_grid_boost", DEFAULT_GRID_BOOST_STARTING_SOC)
            self.manual_grid_boost = data.get("manual_grid_boost", DEFAULT_GRID_BOOST_STARTING_SOC)
            self.calculated_grid_boost = data.get("calculated_grid_boost", DEFAULT_GRID_BOOST_STARTING_SOC)
            self.calculated_grid_boost_day = data.get("calculated_grid_boost_day", "")
            self.min_battery_soc = data.get("min_battery_soc", DEFAULT_GRID_BOOST_MIDNIGHT_SOC)
            self.grid_boost_start = data.get("grid_boost_start", DEFAULT_GRID_BOOST_START)
            self._boost = data.get("_boost", "Testing")
            self.days_of_load_history = data.get("days_of_load_history", DEFAULT_GRID_BOOST_HISTORY)

    async def _save_data(self):
        """Save data to storage."""
        #NOTE: I SHOULD SAVE THIS ANY TIME ANY OF THESE VALUES CHANGE
        data = {
            "actual_grid_boost": self.actual_grid_boost,
            "manual_grid_boost": self.manual_grid_boost,
            "calculated_grid_boost": self.calculated_grid_boost,
            "calculated_grid_boost_day": self.calculated_grid_boost_day,
            "min_battery_soc": self.min_battery_soc,
            "grid_boost_start": self.grid_boost_start,
            "_boost": self._boost,
            "days_of_load_history": self.days_of_load_history,
        }
        await self.store_data._save(data)


    # SERVICE CALLS start here
    async def set_boost(self, boost: str) -> None:
        """Set the boost mode."""
        self._boost = boost
        self.hass.config_entries._update_entry(
            self.config_entry,
            options={**self.config_entry.options, "boost_mode": boost},
        )
        # We need to rerun the hourly updates
        self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))
        await self._hourly_updates()

    async def set_manual_grid_boost(self, manual_grid_boost: int) -> None:
        """Set the manual grid boost value."""
        self.manual_grid_boost = manual_grid_boost
        self.hass.config_entries._update_entry(
            self.config_entry,
            options={
                **self.config_entry.options,
                "manual_grid_boost": manual_grid_boost,
            },
        )
        # We probably need to rerun the hourly updates
        self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))
        await self._hourly_updates()
        # Save the new manual grid boost to storage
        await self._save_data()

    async def set_solcast_percentile(self, percentile: int) -> None:
        """Set the Solcast percentile value."""
        self.solcast_api.percentile = percentile
        self.hass.config_entries._update_entry(
            self.config_entry,
            options={**self.config_entry.options, "percentile": percentile},
        )
        # We need to rerun the hourly updates
        self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))
        await self._hourly_updates()

    async def set_solcast_update_hour(self, update_hour: str) -> None:
        """Set the Solcast update hours."""
        self.solcast_api.update_hour = int(update_hour)
        self.hass.config_entries._update_entry(
            self.config_entry,
            options={**self.config_entry.options, "forecast_hour": update_hour},
        )
        # We need to rerun the hourly updates
        self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))
        await self._hourly_updates()

    async def set_days_of_load_history(self, days_of_load_history: int) -> None:
        """Set the days of load history."""
        self.days_of_load_history = days_of_load_history
        self.hass.config_entries._update_entry(
            self.config_entry,
            options={**self.config_entry.options, "history_days": days_of_load_history},
        )
        # We need to rerun the hourly updates
        self._hourly_task_next_start = datetime.now(ZoneInfo(self.timezone))
        await self._hourly_updates()
        # Save the new days of load history to storage
        await self._save_data()
    # SERVICE CALLS end here

    # Internal methods for data retrieval and calculations
    def _safe_get_ha_sensor(self, entity_id: str) -> float:
        """Safely get the float value of a Home Assistant sensor entity."""
        state_obj = self.hass.states.get(entity_id)
        try:
            if state_obj is None or state_obj.state in (None, "unknown", "unavailable"):
                logger.debug(f"Entity {entity_id} not found or has no valid state.")
            return float(state_obj.state) if state_obj and state_obj.state not in (None, "unknown", "unavailable") else 0.0
        except (ValueError, TypeError):
            logger.warning(f"Invalid value for {entity_id}: {getattr(state_obj, 'state', None)}")
            return 0.0
        
    async def _update_total_battery_capacity(self) -> float:
        """
        Private method to update and store the total battery Ah capacity.
        Sums all Home Assistant sensors matching the sensor 'BATTERY_CAPACITY'.
        """
        now = datetime.now(ZoneInfo(self.timezone))
        if self._battery_capacity_last_updated is not None and self._battery_capacity_last_updated.date() == now.date():
            return  self.battery_capacity   # Already updated today, return the cached value

        entity_ids = [
            entity_id
            for entity_id in self.hass.states._entity_ids()
            if entity_id.startswith(BATTERY_CAPACITY)
        ]
        total_capacity = 0.0
        for entity_id in entity_ids:
            value = self._safe_get_ha_sensor(entity_id)
            total_capacity += value

        logger.debug("Total battery Ah capacity calculated to be: %.2f", total_capacity)
        return total_capacity


    async def _update_boost_settings(
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

        # Call the coordinator and request _request_refresh
        if self.coordinator:
            await self.coordinator._request_refresh()


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

        return await get_instance(self.hass)._add_executor_job(
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
        stats = await get_instance(self.hass)._add_executor_job(
            self._get_statistics_during_period,
            start_of_last_hour,
            end_of_last_hour,
            {entity_id},
        )

        # Extract the mean value for the entity_id
        mean_value = stats.get(entity_id, [{}])[0].get("mean", 0.0)
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

    async def write_grid_boost_soc(self, boost: int) -> None:
        """Set the grid boost value into capacity point 1."""
        if self._boost in {"Testing", "Off"}:
            return
        logger.debug(
            "Trying async method to set grid boost to %s%%.",
            boost,
        )
        my_topic = "sa/inverter_1/capacity_point_1/set"
        service_data = { "payload": str(boost), "topic": my_topic }
        # logger.info("Service data: %s", service_data)
        await self.hass.services._call("mqtt","publish", service_data, False)
        self.actual_grid_boost = boost
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

        # Update shading if we had a positive average PV power, battery soc is low enough to allow charging, and the sun was full
        last_hour = (datetime.now(ZoneInfo(self.timezone)).hour - 1) % 24

        if (
            pv_average > 0
            and self.solcast_api.get_previous_hour_pv_estimate() > 0
            and self.realtime_battery_soc < 96
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
            await self._save_shading()

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
        # If the battery is empty, MQTT is not connected, or the inverter is off, return
        if self.realtime_battery_soc == 0:
            return
        minutes = 0

        # Log for debugging
        logger.debug("------- Calculating remaining battery time -------")
        logger.debug(
            "Starting at %s with %s wH usable energy.",
            printable_hour(hour),
            self.battery_wh_usable,
        )
        while self.battery_wh_usable > 0 and minutes < FIVE_DAYS:
            # For each hour, calculate the impact of the solar generation and load.
            load = (
                self.daily_load_averages.get(hour, 1000) / DEFAULT_INVERTER_EFFICIENCY
            )
            pv = (
                1000
                * self.solcast_api.forecast.get(f"{day}-{hour}", (0.0, 0.0))[0]
                * (1 - self.daily_shading.get(hour, 0.0))
            )
            battery_wh_usable = battery_wh_usable - load + pv
            logger.debug(
                "At %s battery energy is %6s wH.",
                printable_hour((hour + 1) % 24),
                f"{battery_wh_usable:6,.0f}",
            )
            # Monitor progress
            if battery_wh_usable > 0:
                minutes += 60
            else:
                minutes -= int((battery_wh_usable / (pv - load)) * 60)
            # Move to next hour
            hour = (hour + 1) % 24
            if hour == 0:
                day = day + timedelta(days=1)

        self.battery_minutes_remaining = minutes
        if minutes >= FIVE_DAYS:
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
        # Save the new grid boost SoC to storage
        await self._save_data()

    # Private hourly update method (called by hourly task)
    async def _hourly_updates(self) -> None:
        """Update the hourly data for the sensors.

        We need to update the following data:
        - Solcast forecast data
        - Daily shading data
        - Off-peak grid boost SoC
        - Remaining battery life
        """
        # Check if the MQTT connection is established
        if not mqtt.is_connected(self.hass):
            logger.debug(msg="MQTT is not connected. Skipping hourly updates.")
            return
        
        # First, check if this is the right time for an hourly update. Return if not.
        if self._hourly_task_next_start >= datetime.now(ZoneInfo(self.timezone)):
            return
        
        # Get the battery capacity in Ah and update the total battery Ah capacity
        self.battery_capacity = await self._update_total_battery_capacity()

        # Get the realtime data from the MQTT sensors
        self.realtime_battery_soc = self._safe_get_ha_sensor(BATTERY_SOC)
        self.battery_wh_usable = (self.realtime_battery_soc / 100) * self.battery_capacity * 3600

        self.data_updated = datetime.now(ZoneInfo(self.timezone)).strftime(
            "%a %I:%M %p"
        )

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
            await self._save_forecast()
            self._update_tou_boost = False
        # Calculate the off-peak grid boost SoC in case the user changed other settings like days of load history
        # BUT... only do this after the current morning boost period has ended
        if now.hour >= 6:
            await self._calculate_tou_boost_soc()
        # Compute remaining battery life for the user
        await self._calculate_tou_battery_remaining_time()


    # Start the TOU Scheduler - load saved data and start the background tasks
    async def _start(self) -> None:
        """Set up the config options callback when starting the TOU Scheduler."""
        # Ensure config_entry is set
        if not self.config_entry:
            logger.error("Config entry is not set.")
            return
        # Load the data from storage
        await self._load_data()
        await self._load_shading()
        await self._load_forecast()
        # Try to update the data
        await self._hourly_updates() 


    async def to_dict(self) -> dict[str, float | str | datetime]:
        """Return calculated data as a dictionary.

        Returns:
            dict[str, Any]: A dictionary containing the calculated TOU data.
            If MQTT is not connected yet, returns an empty dictionary.
            Realtime data is not included in the dictionary. Dashboards should directly access realtime data.

        """
        # Do hourly updates if needed
        if await self._hourly_updates() is False:
            logger.debug("Unable to get. MQTT may not be connected yet.")
            return {}
        
        # Calculate the estimated time remaining until the battery is exhausted since the last time the battery capacity was updated.
        if self.data_updated is not None:
            # Parse the last update time and set the correct timezone
            data_updated_time = datetime.strptime(self.data_updated, "%a %I:%M %p").replace(
                tzinfo=ZoneInfo(self.timezone)
            )
            # Compute elapsed minutes since last update
            elapsed_minutes = int((datetime.now(ZoneInfo(self.timezone)) - data_updated_time).total_seconds() / 60)
        else:
            logger.debug("Data not updated yet. Returning default values.")

        # Cache the current hour for the current hour load estimate below
        hour = datetime.now(ZoneInfo(self.timezone)).hour

        # Return the data as a dictionary
        return {
            # Battery data
            "battery_time_remaining": self.battery_minutes_remaining / 60 - elapsed_minutes,
            # PV data
            "power_pv_estimated": self.solcast_api.get_previous_hour_pv_estimate(),
            "forecast_today": self.solcast_api.forecast_today,
            "forecast_tomorrow": self.solcast_api.forecast_tomorrow,
            # Inverter info
            "load_estimate": self.load_estimates.get(str(hour), {}).get(hour, 1000),
            # Boost data
            "actual_grid_boost": self.actual_grid_boost,
            "manual_grid_boost": self.manual_grid_boost,
            "grid_boost_mode": self._boost,
            "grid_boost_soc": self.calculated_grid_boost,
            "grid_boost_day": self.calculated_grid_boost_day,
            "grid_boost_start": self.grid_boost_start,
            "grid_boost_on": self._boost,
            "update_hour": self.solcast_api.update_hour,
            # Misc data
            "min_soc": self.min_battery_soc,
            "confidence": self.solcast_api.percentile,
            "load_days": self.days_of_load_history,
            # Daily data
            "shading": str(self.daily_shading),
            "load": str(self.daily_load_averages),
        }
