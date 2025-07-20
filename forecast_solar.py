import aiohttp
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from homeassistant.helpers.storage import Store
from const import FORECAST_SOLAR_API_URL, FORECAST_SOLAR_KEY

logger = logging.getLogger(__name__)

class ForecastSolar:
    def __init__(self, hass):
        """
        panels: List of dicts, each with keys: lat, lon, dec, az, kwp
        timezone: String, e.g. "America/New_York"
        update_hour: Hour of day (local time) to update forecast (default 22 = 10pm)
        """
        self.hass = hass
        self.panels = {}  # Will be set later
        self.timezone = hass.timezone
        self.update_hour = 22 # Default update hour (10pm)
        self.store = Store(hass, version=1, key=FORECAST_SOLAR_KEY)
        self.data = {}
        self.last_updated = None

    async def _fetch_panel_forecast(self, panel):
        url = (FORECAST_SOLAR_API_URL +
            f"{panel['lat']}/{panel['lon']}/{panel['dec']}/{panel['az']}/{panel['kwp']}"
        )
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    return result.get("result", {})
                else:
                    logger.error(f"Failed to fetch forecast.solar for panel {panel}: {resp.status}")
                    return {}

    async def _fetch_all_forecasts(self, panels):
        """Fetch and sum forecasts for all panels by hour."""
        summed = {}
        for panel in panels:
            panel_result = await self._fetch_panel_forecast(panel)
            for hour, value in panel_result.items():
                summed[hour] = summed.get(hour, 0) + value
        return summed

    async def _should_update(self):
        """Determine if we should update (once per day at update_hour)."""
        now = datetime.now(ZoneInfo(self.timezone))
        if self.last_updated is None:
            return True
        last_update = datetime.strptime(self.last_updated, "%Y-%m-%d").replace(tzinfo=ZoneInfo(self.timezone))
        return now.date() > last_update.date() and now.hour >= self.update_hour

    async def _load_data(self):
        """Load forecast data from hass storage."""
        data = await self.store._load()
        if data:
            self.data = data.get("data", {})
            self.last_updated = data.get("last_updated")
        else:
            self.data = {}
            self.last_updated = None

    async def _save_data(self):
        """Save forecast data to hass storage."""
        await self.store._save({
            "data": self.data,
            "last_updated": datetime.now(ZoneInfo(self.timezone)).strftime("%Y-%m-%d")
        })

    async def get_forecast(self, panels, update_hour=22):
        """Main entry: load or update forecast data as needed."""
        self.update_hour = update_hour
        await self._load_data()
        now = datetime.now(ZoneInfo(self.timezone))
        if not self.data or await self._should_update():
            logger.info("Fetching new forecast.solar data...")
            self.data = await self._fetch_all_forecasts(panels)
            self.last_updated = now.strftime("%Y-%m-%d")
            await self._save_data()
        else:
            logger.debug("Loaded forecast.solar data from storage.")
        return self.data