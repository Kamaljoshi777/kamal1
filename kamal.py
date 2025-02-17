from pydantic.json import pydantic_encoder
import json
import orjson
import asyncio
import aioredis
from pydantic import BaseModel
from typing import List
from enum import Enum
import yfinance as yf
import logging
from datetime import datetime

# Set up logging....................
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the StockData model.....................
class StockData(BaseModel):
    symbol: str
    price: float
    volume: int
    timestamp: datetime
    high: float
    low: float
    open: float
    close: float

# Define the AlertType enum..........
class AlertType(Enum):
    PRICE_THRESHOLD = "price_threshold"
    PERCENTAGE_CHANGE = "percentage_change"
    VOLUME_SPIKE = "volume_spike"

# Define the AlertCondition model.................
class AlertCondition(BaseModel):
    symbol: str
    alert_type: AlertType
    threshold: float
    comparison: str

# Define the DataFetcher class..................
class DataFetcher:
    async def fetch_stock_data(self, symbol: str) -> StockData:
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.info
            if data:
                stock_data = StockData(
                    symbol=symbol,
                    price=data['currentPrice'],
                    volume=data['volume'],
                    timestamp=datetime.now(),
                    high=data['dayHigh'],
                    low=data['dayLow'],
                    open=data['open'],
                    close=data['previousClose']
                )
                return stock_data
            else:
                logger.error(f"Failed to fetch stock data for {symbol}: Symbol not found")
                return None
        except Exception as e:
            logger.error(f"Failed to fetch stock data for {symbol}: {e}")
            return None

# Define the CacheManager class..................
class CacheManager:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis = None

    async def connect(self):
        self.redis = await aioredis.from_url(self.redis_url)

    async def get_stock_data(self, symbol: str) -> StockData:
        try:
            data = await self.redis.get(symbol)
            if data:
                return StockData.parse_raw(data)
            else:
                return None
        except Exception as e:
            logger.error(f"Failed to get stock data from cache for {symbol}: {e}")
            return None

    async def set_stock_data(self, symbol: str, data: StockData, ttl: int = 300):
        try:
            stock_data_json = orjson.dumps(data.model_dump())

            await self.redis.set(symbol, stock_data_json,ex = ttl)
        except Exception as e:
            logger.error(f"Failed to set stock data in cache for {symbol}: {e}")


# Define the AlertManager class......
class AlertManager:
    def __init__(self):
        self.alert_conditions = {}

    async def add_alert(self, condition: AlertCondition):
        self.alert_conditions[condition.symbol] = condition

    async def check_alerts(self, stock_data: StockData):
        for symbol, condition in self.alert_conditions.items():
            if symbol == stock_data.symbol:
                if condition.alert_type == AlertType.PRICE_THRESHOLD:
                    if (condition.comparison == "above" and stock_data.price > condition.threshold) or \
                       (condition.comparison == "below" and stock_data.price < condition.threshold):
                        logger.info(f"Alert triggered for {symbol}: {stock_data.price}")

# Define the StockMonitor class.............
class StockMonitor:
    def __init__(self, data_fetcher: DataFetcher, cache_manager: CacheManager, alert_manager: AlertManager):
        self.data_fetcher = data_fetcher
        self.cache_manager = cache_manager
        self.alert_manager = alert_manager
        self.is_running = False

    async def start_monitoring(self, symbols: List[str]):
        await self.cache_manager.connect()
        self.is_running = True
        while self.is_running:
            for symbol in symbols:
                stock_data = await self.data_fetcher.fetch_stock_data(symbol)
                if stock_data:
                    await self.cache_manager.set_stock_data(symbol, stock_data)
                    await self.alert_manager.check_alerts(stock_data)
            await asyncio.sleep(60)

    async def stop_monitoring(self):
        self.is_running = False

async def main():
    data_fetcher = DataFetcher()
    cache_manager = CacheManager("redis://localhost")
    alert_manager = AlertManager()
    monitor = StockMonitor(data_fetcher, cache_manager, alert_manager)

    await alert_manager.add_alert(AlertCondition(
        symbol="AAPL",
        alert_type=AlertType.PRICE_THRESHOLD,
        threshold=150.0,
        comparison="above"
    ))

    await monitor.start_monitoring(["AAPL", "TCS.NS", "RELIANCE.NS"])
    await asyncio.sleep(3600)
    await monitor.stop_monitoring()

if __name__ == "__main__":
    asyncio.run(main())
