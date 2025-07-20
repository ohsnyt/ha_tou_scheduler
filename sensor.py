"""Sensor platform for the TOU Scheduler integration."""

import logging

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfPower
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo  # Correct import
from homeassistant.helpers.entity import generate_entity_id
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DEBUGGING, DOMAIN
from .coordinator import TOUUpdateCoordinator
from .entity import (
    BatteryLifeEntity,
    LoadEntity,
    ShadingEntity,
    TOUBoostEntity,
    TOUSchedulerEntity,
)

logger = logging.getLogger(__name__)
if DEBUGGING:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


class OhSnytSensorEntityDescription(SensorEntityDescription):
    """Describes a sensor."""


TOU_SENSOR_ENTITIES: dict[str, OhSnytSensorEntityDescription] = {
    # "Realtime" power flow related sensors.
    "power_pv_estimated": OhSnytSensorEntityDescription(
        key="power_pv_estimated",
        icon="mdi:flash",
        name="Estimated PV power",
        native_unit_of_measurement=UnitOfPower.WATT,
        device_class=SensorDeviceClass.POWER,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
    ),
    # Grid boost related sensors.
    "grid_boost_soc": OhSnytSensorEntityDescription(
        key="grid_boost_soc",
        icon="mdi:battery",
        name="Calculated Grid Boost SoC",
        native_unit_of_measurement="%",
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
    ),
    "actual_grid_boost": OhSnytSensorEntityDescription(
        key="actual_grid_boost",
        icon="mdi:battery",
        name="Actual Grid Boost SoC",
        native_unit_of_measurement="%",
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
    ),
    "manual_grid_boost": OhSnytSensorEntityDescription(
        key="manual_grid_boost",
        icon="mdi:battery",
        name="Manual Grid Boost SoC",
        native_unit_of_measurement="%",
        device_class=SensorDeviceClass.BATTERY,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
    ),
    # Battery related sensors.
    "battery_time_remaining": OhSnytSensorEntityDescription(
        key="battery_time_remaining",
        icon="mdi:timer-outline",
        name="Battery Time Remaining",
        native_unit_of_measurement="h",
        device_class=SensorDeviceClass.DURATION,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
    ),
}


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up individual sensors.

    Since the plant_id will never change even if we recreate the coordinator,
    we can use it as the stable element to build sensor keys.
    The plant_id is defined before sensors are created.
    """

    # coordinator = hass.data[DOMAIN].get(entry.entry_id)
    coordinator = hass.data[DOMAIN][entry.entry_id]["coordinator"]
    # Double check that we have data
    if coordinator is None:
        logger.error("Coordinator is missing from hass.data")
        return

    # NOTE: I was trying to set up a parent entity for all the sensors, but it was not working.
    # Since the plant_id is stable, we can use it as the unique identifier unique_prefix for all sensors.
    plant_id = "131031"
    if plant_id is None:
        logger.error("This never happens: Plant ID is missing from coordinator data.")
        return
    unique_prefix = f"{plant_id}_tou"
    parent = f"{unique_prefix}_scheduler"

    # Add special entity sensors: Scheduler, Battery, Cloud, Plant, Inverter, Shading and Load (from entity.py)
    entity_list = [
        TOUSchedulerEntity,
        ShadingEntity,
        LoadEntity,
        BatteryLifeEntity,
        TOUBoostEntity,
    ]
    entities = [
        entity(entry_id=unique_prefix, coordinator=coordinator)
        for entity in entity_list
    ]
    async_add_entities(entities)

    # Add the "normal" Sol-Ark sensors for the inverter (from this file)
    sensors = [
        OhSnytSensor(
            entry_id=unique_prefix,
            coordinator=coordinator,
            parent=parent,
            description=entity_description,
        )
        for entity_description in TOU_SENSOR_ENTITIES.values()
    ]
    async_add_entities(sensors)


class OhSnytSensor(CoordinatorEntity[TOUUpdateCoordinator], SensorEntity):
    """Representation of a standard sensor."""

    #has_entity_name = False  # Prevent Home Assistant from generating a friendly name

    def __init__(
        self,
        *,
        entry_id: str,
        parent: str,
        coordinator: TOUUpdateCoordinator,
        description: OhSnytSensorEntityDescription,
    ) -> None:
        """Initialize the sensor."""
        super().__init__(coordinator)
        self.entity_description = description
        self._key = description.key
        self._attr_unique_id = f"{entry_id}_{description.key}"
        icon = description.icon if isinstance(description.icon, str) else "mdi:flash"
        self._attr_icon = icon
        name = description.name if isinstance(description.name, str) else "Unknown"
        self._attr_name = name
        self._attr_native_unit_of_measurement = description.native_unit_of_measurement
        self._attr_device_class = description.device_class
        self._attr_state_class = description.state_class
        self._attr_device_info = {
            "identifiers": {(DOMAIN, entry_id)},
            "name": self._attr_name,
            # via_device=(DOMAIN, parent) if parent else ("", ""),
        }
        self.entity_id = generate_entity_id(
            "sensor.{}", self._attr_unique_id, hass=coordinator.hass
        )
        # logger.debug(
        #     "\n++Created sensor: %s. Native value is: %s %s. (entity_id: %s, _attr_unique_id: %s)",
        #     self._attr_name,
        #     self.native_value,
        #     self._attr_native_unit_of_measurement,
        #     self.entity_id,
        #     self._attr_unique_id,
        # )

    @property
    def name(self) -> str | None:
        """Return the name of the sensor."""
        if self.entity_description.key == "grid_boost_soc":
            day = self.coordinator.data.get("grid_boost_day")
            if day:
                return f"{self._attr_name or ''} ({day})"
        return self._attr_name

    @property
    def unique_id(self) -> str | None:
        """Return a unique ID."""
        return self._attr_unique_id

    @property
    def native_value(self) -> str | int | float | None:
        """Return the state of the sensor."""
        value = self.coordinator.data.get(self.entity_description.key)
        if not isinstance(value, (int, float, type(None))):
            logger.error("Invalid type for native_value: %s (%s)", value, type(value))
            return None
        return value if isinstance(value, float) else value

    @property
    def device_info(self) -> DeviceInfo | None:
        """Return the device info."""
        return self._attr_device_info
