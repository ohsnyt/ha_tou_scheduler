"""Entity classes for TOU Scheduler entity.

This module defines various entity classes used in the TOU (Time of Use) Scheduler integration.
(These are specialized sensors. They share some similarity with the sensors, notably the use of "im_a" to identify the entity type.)
Each entity class represents a different aspect of the TOU Scheduler that is of special interest to the user:
    the scheduler,
    the calculated daily shading, and
    the calculated average daily load.
"""

import datetime
import logging
import re
from typing import Any

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
)

from .const import DEBUGGING, DOMAIN

logger = logging.getLogger(__name__)
if DEBUGGING:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


# Helper functions
def printable_hour(hour: int) -> str:
    """Return a printable hour string in 12-hour format with 'am' or 'pm' suffix.

    Args:
        hour: Hour in 24-hour format (0-23).

    Returns:
        Formatted string in 12-hour format with am/pm.

    """
    return f"{(hour % 12) or 12}" f"{' am' if hour < 12 else ' pm'}"


def parse_percent_data(input_str: str) -> dict[str, str]:
    """Convert a string representation of a dictionary to an actual dictionary.

    Args:
        input_str: String representation of a dictionary.

    Returns:
        A dictionary with integer keys and float values.

    """
    # Remove the curly braces
    input_str = input_str.strip("{}")
    # Split the string into key-value pairs
    pairs = re.split(r",\s*(?![^{}]*\})", input_str)
    result = {}
    for pair in pairs:
        # Split each pair into key and value
        key, value = pair.split(":")
        # Convert key and value to appropriate types and add to the dictionary
        result[printable_hour(int(key.strip()))] = f"{100 * float(value.strip()):.0f}%"
    return result


def parse_wh_data(input_str: str) -> dict[str, str]:
    """Convert a string representation of a dictionary to an actual dictionary.

    Args:
        input_str: String representation of a dictionary.

    Returns:
        A dictionary with integer keys and float values.

    """
    # Remove the curly braces
    input_str = input_str.strip("{}")
    if input_str == "":
        return {}
    # Split the string into key-value pairs
    pairs = re.split(r",\s*(?![^{}]*\})", input_str)
    result = {}
    for pair in pairs:
        # Split each pair into key and value
        key, value = pair.split(":")
        # Convert key and value to appropriate types and add to the dictionary
        result[printable_hour(int(key.strip()))] = f"{float(value.strip()):,.0f} wH"
    return result


def count_data(input_str: str) -> int:
    """Convert a string representation of a dictionary to an actual dictionary.

    Args:
        input_str: String representation of a dictionary.

    Returns:
        A dictionary with integer keys and float values.

    """
    # Remove the curly braces
    input_str = input_str.strip("{}")
    # Split the string into key-value pairs
    pairs = re.split(r",\s*(?![^{}]*\})", input_str)
    result = 0
    for pair in pairs:
        # Split each pair into key and value
        key, value = pair.split(":")
        # Convert key and value to appropriate types and add to the dictionary
        if float(value.strip()) > 0.0:
            result += 1
    return result


def sum_data(input_str: str) -> int:
    """Convert a string representation of a dictionary to an actual dictionary.

    Args:
        input_str: String representation of a dictionary.

    Returns:
        A dictionary with integer keys and float values.

    """
    # Remove the curly braces
    input_str = input_str.strip("{}")
    # Check for empty data
    if input_str == "":
        return 0
    # Split the string into key-value pairs
    pairs = re.split(r",\s*(?![^{}]*\})", input_str)
    result = 0.0
    for pair in pairs:
        # Split each pair into key and value
        key, value = pair.split(":")
        # Convert key and value to appropriate types and add to the dictionary
        result += float(value.strip())
    return int(round(result, 0))


class TOUSchedulerEntity(CoordinatorEntity):
    """Class for TOU Scheduler entity."""

    def __init__(
        self,
        entry_id: str,
        coordinator: DataUpdateCoordinator[dict[str, Any]],
        # parent: str,
    ) -> None:
        """Initialize the sensor."""
        im_a: str = "scheduler"

        super().__init__(coordinator)
        self._coordinator = coordinator
        plant_name: str = self._coordinator.data.get("plant_name", "My plant")
        self._key = im_a
        self._attr_unique_id = f"{entry_id}_{self._key}"
        self._attr_icon = "mdi:toggle-switch"
        self._attr_name = f"{plant_name} ToU {im_a}"
        self._device_info = DeviceInfo(
            identifiers={(DOMAIN, entry_id)},
            name=self._attr_name,
        )

    @property
    def extra_state_attributes(self) -> dict[str, str]:
        """Return the extra state attributes."""
        soc = self.coordinator.data.get("batt_soc", 0)
        soc_value = f"{int(soc)}%" if soc else "Unknown"
        today: str = datetime.datetime.now().strftime("%a")
        tomorrow: str = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime(
            "%a"
        )

        return {
            "plant_name": self.coordinator.data.get("plant_name", "Plant name n/a"),
            "battery_soc": soc_value,
            "inverter_model": self.coordinator.data.get(
            "inverter_model", "Inverter model n/a"
            ),
            "cloud_token_refresh": self.coordinator.data.get(
            "bearer_token_expires_on", "Unknown"
            ),
            "cloud_status": self.coordinator.data.get("cloud_status", "Unknown"),
            "plant_status": self.coordinator.data.get(
            "plant_status", "Plant status n/a"
            ),
            "plant_image_url": self.coordinator.data.get("plant_image_url", ""),
            "plant_created": self.coordinator.data.get(
            "plant_created", "Plant created time n/a"
            ),
            "inverter_status": self.coordinator.data.get(
            "inverter_status", "Inverter status n/a"
            ),
            "manual": self.coordinator.data.get("manual_grid_boost", 30),
            "calculated": self.coordinator.data.get("calculated_boost", 20),
            "confidence": self.coordinator.data.get("confidence", 10),
            "min_soc": self.coordinator.data.get("min_soc", 20),
            "load_days": self.coordinator.data.get("load_days", 3),
            "update_hour": self.coordinator.data.get("update_hour", 3),
            "forecast_today": "("
            + today
            + ") "
            + str(round(self.coordinator.data.get("forecast_today", 0.0), 1)),
            "forecast_tomorrow": "("
            + tomorrow
            + ") "
            + str(round(self.coordinator.data.get("forecast_tomorrow", 0.0), 1)),
        }

    @property
    def name(self) -> str | None:
        """Return the name of the sensor."""
        return self._attr_name

    @property
    def unique_id(self) -> str | None:
        """Return a unique ID."""
        return self._attr_unique_id

    @property
    def state(self) -> str:
        """Return the state of the sensor."""
        return self.coordinator.data.get("grid_boost_mode", "State unknown")


class ShadingEntity(CoordinatorEntity):
    """Representation of a Shading.

    This sensor is used to display the shading ratio for each hour of the day if available.
    If there is no sun expected at a certain hour, no ratio will be listed.
    If we are unable to get the shading ratio, the sensor will display "No shading percentages available".
    """

    def __init__(
        self,
        entry_id: str,
        coordinator: DataUpdateCoordinator[dict[str, Any]],
        # parent: str,
    ) -> None:
        """Initialize the sensor."""
        im_a = "shading"

        super().__init__(coordinator)
        self._coordinator = coordinator
        plant_name: str = self._coordinator.data.get("plant_name", "My plant")
        self._key = im_a
        self._attr_unique_id = f"{entry_id}_{self._key}"
        self._attr_icon = "mdi:toggle-switch"
        self._attr_name = f"{plant_name} ToU {im_a}"
        self._count: int = 0
        self._device_info = DeviceInfo(
            identifiers={(DOMAIN, entry_id)},
            name=self._attr_name,
        )

    @property
    def name(self) -> str | None:
        """Return the name of the sensor."""
        return self._attr_name

    @property
    def unique_id(self) -> str | None:
        """Return a unique ID."""
        return self._attr_unique_id

    @property
    def device_info(self) -> DeviceInfo:
        """Return the device info."""
        return self._device_info

    @property
    def extra_state_attributes(self) -> dict[str, str]:
        """Return the hourly shade values as dict[str,str]."""
        hours_str = self._coordinator.data.get("shading", "{}")
        hours: dict[str, str] = parse_percent_data(hours_str)
        if not hours:
            day = datetime.datetime.now().strftime("%a")
            return {"No shading found": day}
        return hours

    @property
    def state(self) -> str | int | float | None:
        """Return the count of hours with shading."""
        hours_str = self._coordinator.data.get("shading", "{}")
        # Count the number of hours with shading
        count: int = count_data(hours_str)
        if count == 1:
            return "1 hour with shading"
        if count > 0:
            return f"{count} hours with shading"
        return "No shading found"


class LoadEntity(CoordinatorEntity):
    """Representation of the average daily load.

    This sensor is used to display the average daily load for each hour of the day if available.
    """

    def __init__(
        self,
        entry_id: str,
        coordinator: DataUpdateCoordinator[dict[str, Any]],
        # parent: str,
    ) -> None:
        """Initialize the sensor."""
        im_a = "load"

        super().__init__(coordinator)
        self._coordinator = coordinator
        plant_name: str = self._coordinator.data.get("plant_name", "My plant")
        self._key = im_a
        self._attr_unique_id = f"{entry_id}_{self._key}"
        self._attr_icon = "mdi:toggle-switch"
        self._attr_name = f"{plant_name} ToU {im_a}"
        self._device_info = DeviceInfo(
            identifiers={(DOMAIN, entry_id)},
            name=self._attr_name,
        )

    @property
    def name(self) -> str | None:
        """Return the name of the sensor."""
        return self._attr_name

    @property
    def unique_id(self) -> str | None:
        """Return a unique ID."""
        return self._attr_unique_id

    @property
    def device_info(self) -> DeviceInfo:
        """Return the device info."""
        return self._device_info

    @property
    def extra_state_attributes(self) -> dict[str, str]:
        """Return the hourly load values as dict[str,str]."""
        hours_str: str = self._coordinator.data.get("load", "{}")
        hours: dict[str, str] = parse_wh_data(hours_str)

        if not hours:
            day = datetime.datetime.now().strftime("%a")
            return {"No load averages found": day}
        return hours

    @property
    def state(self) -> str | int | float | None:
        """Return the state of the sensor."""
        hours_str: str = self._coordinator.data.get("load", "{}")
        load = round(sum_data(hours_str) / 1000, 1)

        return f"{load} kWh"


class BatteryLifeEntity(CoordinatorEntity):
    """Representation of the average daily load.

    This sensor is used to display the average daily load for each hour of the day if available.
    """

    def __init__(
        self,
        entry_id: str,
        coordinator: DataUpdateCoordinator[dict[str, Any]],
        # parent: str,
    ) -> None:
        """Initialize the sensor."""
        im_a = "battery_life"

        super().__init__(coordinator)
        self._coordinator = coordinator
        # plant_name: str = self._coordinator.data.get("plant_name", "My plant")
        self._key = im_a
        self._attr_unique_id = f"{entry_id}_{self._key}"
        self._attr_icon = "mdi:clock-alert"
        self._attr_name = "Battery empty at"
        self._device_info = DeviceInfo(
            identifiers={(DOMAIN, entry_id)},
            name=self._attr_name,
        )

    @property
    def name(self) -> str | None:
        """Return the name of the sensor."""
        return self._attr_name

    @property
    def unique_id(self) -> str | None:
        """Return a unique ID."""
        return self._attr_unique_id

    @property
    def device_info(self) -> DeviceInfo:
        """Return the device info."""
        return self._device_info

    @property
    def state(self) -> str | int | float | None:
        """Return the state of the sensor."""
        eol: str = self._coordinator.data.get(
            "batt_exhausted", datetime.datetime.now().timestamp()
        ).strftime("%a %-I:%M %p")

        return eol


class TOUBoostEntity(CoordinatorEntity):
    """Class for TOU Scheduler entity."""

    def __init__(
        self,
        entry_id: str,
        coordinator: DataUpdateCoordinator[dict[str, Any]],
        # parent: str,
    ) -> None:
        """Initialize the sensor."""
        im_a: str = "boost"

        super().__init__(coordinator)
        self._coordinator = coordinator
        plant_name: str = self._coordinator.data.get("plant_name", "My plant")
        self._key = im_a
        self._attr_unique_id = f"{entry_id}_{self._key}"
        self._attr_icon = "mdi:toggle-switch"
        self._attr_name = f"{plant_name} ToU {im_a}"
        self._device_info = DeviceInfo(
            identifiers={(DOMAIN, entry_id)},
            name=self._attr_name,
        )

    @property
    def extra_state_attributes(self) -> dict[str, str]:
        """Return the extra state attributes."""
        return {
            "mode": self.coordinator.data.get("grid_boost_mode", 20),
            "calculated": self.coordinator.data.get("calculated_boost", 20),
            "confidence": self.coordinator.data.get("confidence", 10),
            "min_soc": self.coordinator.data.get("min_soc", 20),
            "load_days": self.coordinator.data.get("load_days", 3),
            "update_hour": self.coordinator.data.get("forecast_hour", 23),
        }

    @property
    def name(self) -> str | None:
        """Return the name of the sensor."""
        return self._attr_name

    @property
    def unique_id(self) -> str | None:
        """Return a unique ID."""
        return self._attr_unique_id

    @property
    def state(self) -> str:
        """Return the state of the sensor."""
        return self.coordinator.data.get("manual_boost", 30)
