"""Config flow for Time of Use Scheduler."""

from collections.abc import Mapping
import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback

from .const import DOMAIN
from .solcast_api import SolcastAPI, SolcastStatus

_logger = logging.getLogger(__name__)


DATA_SCHEMA_STEP_2 = vol.Schema(
    {
        vol.Required("api_key"): str,
        vol.Required("resource_id"): str,
    }
)


# Helper functions to get options from the user
def int_list_to_string(int_list) -> str:
    """Convert a list of integers to a string."""
    return ", ".join(map(str, int_list))


def string_to_int_list(string_list) -> list[int]:
    """Convert a string containing one or more integers into a list of ints."""
    return [int(i.strip()) for i in string_list.split(",") if i.strip().isdigit()]


def get_options_schema(options: Mapping[str, Any]) -> vol.Schema:
    """Return the options schema."""
    boost = options.get("boost_mode", "testing")
    forecast_hour = options.get("forecast_hour", "23")
    manual_grid_boost = options.get("manual_grid_boost", 50)
    history_days = options.get("history_days", "3")
    min_battery_soc = options.get("min_battery_soc", 15)
    percentile = options.get("percentile", 25)

    return vol.Schema(
        {
            vol.Required("boost", default=boost): vol.In(
                ["automatic", "manual", "off", "testing"]
            ),
            # Manual Settings
            vol.Required("manual_grid_boost", default=manual_grid_boost): vol.All(
                vol.Coerce(int), vol.Range(min=5, max=100)
            ),
            # Automatic (calculated) Settings
            vol.Required("history_days", default=history_days): vol.In(
                ["1", "2", "3", "4", "5", "6", "7"]
            ),
            vol.Required("forecast_hour", default=forecast_hour): str,
            vol.Required("min_battery_soc", default=min_battery_soc): vol.All(
                vol.Coerce(int), vol.Range(min=5, max=100)
            ),
            vol.Required("percentile", default=percentile): vol.All(
                vol.Coerce(int), vol.Range(min=10, max=90)
            ),
        }
    )


class TOUSchedulerConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for TOU Scheduler."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self.api_key: str | None = None
        self.resource_id: str | None = None

    async def async_step_user(
        self, user_input=None
    ) -> config_entries.ConfigFlowResult:
        """Handle the second step of the config flow."""
        errors: dict[Any, Any] = {}
        if user_input is not None:
            self.api_key = user_input.get("api_key")
            self.resource_id = user_input.get("resource_id")
            if self.api_key and self.resource_id:
                # Test the new credentials
                timezone = self.hass.config.time_zone or "UTC"
                solcast = SolcastAPI(self.api_key, self.resource_id, timezone)
                await solcast.refresh_data()
                if solcast.status == SolcastStatus.UNKNOWN:
                    errors["base"] = "invalid_solcast_auth"
                else:
                    # If authentication is successful, proceed to get the options
                    return await self.async_step_parameters()
            else:
                errors["base"] = "missing_solcast_credentials"

        return self.async_show_form(
            step_id="user", data_schema=DATA_SCHEMA_STEP_2, errors=errors
        )

    async def async_step_parameters(
        self, user_input=None
    ) -> config_entries.ConfigFlowResult:
        """Handle the third step of the config flow."""
        errors: dict[Any, Any] = {}
        if user_input is not None:

            # Save the user input and create the config entry
            if not errors:
                return self.async_create_entry(
                    title="TOU Scheduler",
                    data={
                        "api_key": self.api_key,
                        "resource_id": self.resource_id,
                        "forecast_hour": user_input["forecast_hour"],
                        "manual_grid_boost": user_input["manual_grid_boost"],
                        "history_days": user_input["history_days"],
                        "min_battery_soc": user_input["min_battery_soc"],
                        "percentile": user_input["percentile"],
                        "boost_mode": user_input["boost"],
                    },
                )

        return self.async_show_form(
            step_id="parameters",
            data_schema=get_options_schema(user_input or {}),
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Get the options flow."""
        return TouSchedulerOptionFlow(config_entry)


class TouSchedulerOptionFlow(config_entries.OptionsFlow):
    """Handle TOU Scheduler options."""

    def __init__(self, config_entry) -> None:
        """Initialize options flow."""
        # self.config_entry = config_entry

    async def async_step_init(self, user_input=None) -> config_entries.ConfigFlowResult:
        """Redo the parameters step of the options flow."""
        errors: dict[Any, Any] = {}
        if user_input is not None:
            # Save the user input and update the config entry options, converting the pseudo list to a list
            if not errors:
                # Update the config entry options
                self.hass.config_entries.async_update_entry(
                    self.config_entry,
                    options={
                        "manual_grid_boost": user_input["manual_grid_boost"],
                        "history_days": user_input["history_days"],
                        "forecast_hour": user_input["forecast_hour"],
                        "min_battery_soc": user_input["min_battery_soc"],
                        "percentile": user_input["percentile"],
                        "boost_mode": user_input["boost"],
                    },
                )
                # Get the coordinator and request a refresh
                coordinator = self.hass.data[DOMAIN][self.config_entry.entry_id]
                await coordinator.async_request_refresh()
                return self.async_create_entry(
                    title="",
                    data={
                        "manual_grid_boost": user_input["manual_grid_boost"],
                        "history_days": user_input["history_days"],
                        "forecast_hour": user_input["forecast_hour"],
                        "min_battery_soc": user_input["min_battery_soc"],
                        "percentile": user_input["percentile"],
                        "boost_mode": user_input["boost"],
                    },
                )

        return self.async_show_form(
            step_id="init",
            data_schema=get_options_schema(self.config_entry.options),
            errors=errors,
        )
