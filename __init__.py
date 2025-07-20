"""The Time of Use Scheduler integration."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, ServiceCall

# from homeassistant.helpers.typing import ConfigType
from .const import DOMAIN, PLATFORMS
from .coordinator import TOUUpdateCoordinator
from .solcast_api import SolcastAPI
from .tou_scheduler import TOUScheduler

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up TOU Scheduler from a config entry."""
    try:
        # Initialize the Solcast API
        solcast_api = SolcastAPI(
            api_key=entry.data.get("api_key", None),
            resource_id=entry.data.get("resource_id", None),
            timezone=hass.config.time_zone,
        )
        _LOGGER.debug(
            "Solcast API initialized with resource_id: %s and api_key: %s",
            entry.data.get("resource_id", None),
            entry.data.get("api_key", None),
        )

        # Initialize the TOU Scheduler
        tou_scheduler = TOUScheduler(
            hass=hass,
            config_entry=entry,
            timezone=hass.config.time_zone,
            solcast_api=solcast_api,
            coordinator=None,  # Temporarily set to None
        )
        await tou_scheduler.async_start()
        _LOGGER.debug("TOU Scheduler initialized with timezone: %s", hass.config.time_zone)

        # Create the UpdateCoordinator
        coordinator = TOUUpdateCoordinator(
            hass=hass,
            update_method=tou_scheduler.to_dict,
        )
        await coordinator.async_config_entry_first_refresh()

        # Assign the coordinator to the TOUScheduler instance
        tou_scheduler.coordinator = coordinator

        # Store the TOUScheduler instance in hass.data[DOMAIN]
        hass.data.setdefault(DOMAIN, {})[entry.entry_id] = {
            "coordinator": coordinator,
            "tou_scheduler": tou_scheduler,
        }

        # Forward the setup to the sensor platform
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

        # Register custom services
        async def handle_set_boost(call: ServiceCall) -> None:
            """Handle the set_boost service call."""
            boost = call.data["boost"]
            await tou_scheduler.set_boost(boost)

        async def handle_set_manual_grid_boost(call: ServiceCall) -> None:
            """Handle the set_manual_grid_boost service call."""
            manual_grid_boost = call.data["manual_grid_boost"]
            await tou_scheduler.set_manual_grid_boost(manual_grid_boost)

        async def handle_set_solcast_percentile(call: ServiceCall) -> None:
            """Handle the set_solcast_percentile service call."""
            percentile = call.data["percentile"]
            await tou_scheduler.set_solcast_percentile(percentile)

        async def handle_set_solcast_update_hours(call: ServiceCall) -> None:
            """Handle the set_solcast_update_hours service call."""
            update_hour = call.data["update_hour"]
            await tou_scheduler.set_solcast_update_hour(update_hour)

        async def handle_set_days_of_load_history(call: ServiceCall) -> None:
            """Handle the set_days_of_load_history service call."""
            days_of_load_history = call.data["days_of_load_history"]
            await tou_scheduler.set_days_of_load_history(days_of_load_history)

        async def handle_service_settings(call: ServiceCall) -> None:
            """Accept all settings values and update the integration."""
            boost_mode: str = call.data.get("boost_mode", "manual")
            confidence: int = call.data.get("confidence", 25)
            load_days: int = call.data.get("load_days", 7)
            manual_grid_boost: int = call.data.get("manual_grid_boost", 50)
            min_battery_soc: int = call.data.get("min_battery_soc", 15)
            update_hour: int = call.data.get("update_hour", 23)

            # Access the TOUScheduler instance from hass.data[DOMAIN]
            tou_scheduler = hass.data[DOMAIN][entry.entry_id]["tou_scheduler"]

            # Update the TOU Scheduler entity with the new settings
            await tou_scheduler.async_update_boost_settings(
                boost_mode,
                manual_grid_boost,
                min_battery_soc,
                confidence,
                load_days,
                update_hour,
            )

        hass.services.async_register(DOMAIN, "set_boost", handle_set_boost)
        hass.services.async_register(DOMAIN, "set_manual_grid_boost", handle_set_manual_grid_boost)
        hass.services.async_register(DOMAIN, "set_solcast_percentile", handle_set_solcast_percentile)
        hass.services.async_register(DOMAIN, "set_solcast_update_hour", handle_set_solcast_update_hours)
        hass.services.async_register(DOMAIN, "set_days_of_load_history", handle_set_days_of_load_history)
        hass.services.async_register(DOMAIN, "set_boost_settings", handle_service_settings)

    except Exception as e:  # noqa: BLE001
        _LOGGER.error("Error setting up TOU Scheduler entry: %s", e)
        return False

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload the config entry."""
    _LOGGER.error("Unloading TOU Scheduler entry: %s", entry.entry_id)
    try:
        coordinator = hass.data[DOMAIN].get(entry.entry_id)
        if coordinator:
            await coordinator.tou_scheduler.close_session()
        unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
        if unload_ok:
            hass.data[DOMAIN].pop(entry.entry_id)
    except KeyError as e:
        _LOGGER.error("Error unloading TOU Scheduler entry: %s", e)
        return False
    return unload_ok
