"""Constants for the Tou Scheduler integration."""

from aiohttp import ClientTimeout

from homeassistant.const import Platform

# Basic integration details.
DOMAIN = "tou_scheduler"
PLATFORMS = [Platform.SENSOR]
# Version of the Tou Scheduler integration
VERSION = "0.5.0"

# Turn on integration detailed debugging logging (True)
DEBUGGING = True

# Define a key for storing the coordinator in hass.data
COORDINATOR_KEY = "tou_coordinator"
# Storage keys for the Tou Scheduler data
SHADE_KEY = "tou_scheduler_storage"
FORECAST_KEY = "tou_scheduler_forecast"
DATA_KEY = "tou_scheduler_data"
FORECAST_SOLAR_KEY = "forecast_solar_data"

# Define the common names for the inverter models
SOLARK_MODEL_TO_NAME = {
    "STROG INV": "Sol-Ark 12K-2P-N",
}

# Grid boost off and on, as strings reported back by the cloud
OFF = "False"
ON = "True"

# Solar Assistant constants
PV_POWER = "sensor.deye_sunsynk_sol_ark_pv_power"
BATTERY_POWER = "sensor.deye_sunsynk_sol_ark_battery_power"
LOAD_POWER = "sensor.deye_sunsynk_sol_ark_load_power"
BATTERY_SOC = "sensor.deye_sunsynk_sol_ark_battery_state_of_charge"
FLOAT_VOLTAGE = "sensor.deye_sunsynk_sol_ark_battery_float_charge_voltage"
GRID_BOOST = "number.deye_sunsynk_sol_ark_capacity_point_1"
BATTERY_CAPACITY = "sensor.deye_sunsynk_sol_ark_battery_capacity"

# Battery constants
BATTERY_AH = 300
BATTERY_MIN_SOC = 2
BATTERY_MAX_WH = BATTERY_AH * 56.2
BATTERY_LOST_WH = BATTERY_MIN_SOC/100 * BATTERY_AH * 56.2
BATTERY_WH_PER_PERCENT = BATTERY_MAX_WH / 100
FIVE_DAYS = 5 * 24 * 60 # in minutes

# MQTT sensor names for Solar Assistant
LOAD = "sensor.deye_sunsynk_sol_ark_load_power"
PV = "sensor.deye_sunsynk_sol_ark_pv_power"
BATTERY_POWER = "sensor.deye_sunsynk_sol_ark_battery_power"
GRID_POWER = "sensor.deye_sunsynk_sol_ark_grid_power"
LOAD_POWER = "sensor.deye_sunsynk_sol_ark_load_power"
PV_POWER = "sensor.deye_sunsynk_sol_ark_pv_power"
GRID_BOOST_SET = "sensor.deye_sunsynk_sol_ark_battery_capacity_point_1"
MIN_BATTERY_SOC = "sensor.deye_sunsynk_sol_ark_battery_stop_discharge_capacity"
BATTERY_SOC = "sensor.deye_sunsynk_sol_ark_battery_state_of_charge"

# URL for the forecast solar API
FORECAST_SOLAR_API_URL = "https://api.forecast.solar/estimate/watts/"

# Default values for the Tou Scheduler configuration
DEFAULT_INVERTER_EFFICIENCY = 0.85
DEFAULT_LOAD_ESTIMATE = 1000
MAX_SOC = 99
MIN_SOC_FOR_CHARGING = 96


# Configuration option keys, to reduce typing mistakes in the code
UPDATE_INTERVAL = 1  # Update interval in minutes
BOOST_OPTIONS = "boost_options"
SOLCAST_API_KEY = "api_key"
SOLCAST_RESOURCE_ID = "resource_id"
GRID_BOOST_HISTORY = "history_days"
GRID_BOOST_SOC_HIGH = 60
GRID_BOOST_ON = "boost_calculation"
GRID_BOOST_ON_OPTIONS = {"on": "On", "off": "Off"}
GRID_BOOST_MIDNIGHT_SOC = "min_battery_soc"
# GRID_BOOST_STARTING_SOC = "grid_boost_starting_soc"
SOLCAST_PERCENTILE = "percentile"
SOLCAST_UPDATE_HOUR = "forecast_hour"
TIMEOUT: ClientTimeout = ClientTimeout(total=30)  # 30 second timeout for all requests
TIMEZONE = "timezone"

# NOTE: Will Prouse recommends that in a solar system, the battery can cycle from 0% to 100% each day with no ill effects.
#       Therefore we allow the battery to discharge to 1% (in extraordinary circumstances). We also only try to keep enough
#       power to have 10% left at the end of the day. This allows us to maximize how much power we generate by PV, and
#       minimize how much power we require from the grid.
#
#       In my circumstance, off-peak rates are midnight to 6 am. We therefore start off-peak charging at a few minutes
#       past midnight
#       Solcast estimates may be requested up to (about) 8 times per day. I default to request only once at 11pm. Estimates
#       are returned for 10 percentile, 50 percentile and 90 percentile forecasts. Through interpolation I allow the
#       user to select any value in between. I default to the 25th percentile.
GRID_BOOST_SOC_LOW = (
    1  # Min battery level before forcing grid charging any time of the day
)
DEFAULT_BATTERY_SHUTDOWN = 1  # 1% is the lowest we allow the battery to discharge
DEFAULT_GRID_BOOST_STARTING_SOC = (
    25  # Min battery level at the end of the off-peak charging time
)
DEFAULT_GRID_BOOST_MIDNIGHT_SOC = 10  # 20% provides some buffer to keep the battery from dipping below 15% during the day
SOLCAST_PERCENTILE_LOW = 10
SOLCAST_PERCENTILE_HIGH = 90
DEFAULT_GRID_BOOST = "Off"
DEFAULT_GRID_BOOST_START = "00:03"
DEFAULT_GRID_BOOST_END = "06:00"
DEFAULT_SOLCAST_PERCENTILE = 15
DEFAULT_MANUAL_GRID_BOOST = 50
DEFAULT_SOLCAST_UPDATE_HOUR = 23
DEFAULT_INVERTER_MIN_SOC = 10
DEFAULT_GRID_BOOST_HISTORY = 3

# Grid boost history strings to make the user interface easier to understand
GRID_BOOST_HISTORY_OPTIONS = {
    1: "1 day",
    2: "2 days",
    3: "3 days",
    4: "4 days",
    5: "5 days",
    6: "6 days",
    7: "7 days",
    8: "8 days",
    9: "9 days",
    10: "10 days",
    11: "11 days",
    12: "12 days",
    13: "13 days",
    14: "14 days",
}
