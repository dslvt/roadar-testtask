from typing import Union
from fastapi import FastAPI
from pydantic import BaseModel
from datetime import date, datetime, time, timedelta, timezone
import functools
import json
import logging
from typing import Dict, Iterable, List, Tuple, Union

import aioredis
import httpx
from aioredis.exceptions import ResponseError
from fastapi import BackgroundTasks, Depends, FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseSettings
import json
import asyncio


class Config(BaseSettings):
    # The default URL expects the app to run using Docker and docker-compose.
    redis_url: str = "redis://redis:6379"
    # redis_url: str = 'redis://localhost:6379'


DEFAULT_KEY_PREFIX = "sensor-data"
BitcoinSentiments = List[Dict[str, Union[str, float]]]

log = logging.getLogger(__name__)
config = Config()
app = FastAPI(title="Roadar Test Task")
redis = aioredis.from_url(config.redis_url, decode_responses=True)


FIVE_SEC = 5
FIVE_SEC_BUCKET = "5000"


def make_keys():
    return Keys()


class SensorItem(BaseModel):
    datetime: datetime
    payload: int

class ManipulatorItem(BaseModel):
    datetime: datetime
    status: str

def prefixed_key(f):
    """
    A method decorator that prefixes return values.
    Prefixes any string that the decorated method `f` returns with the value of
    the `prefix` attribute on the owner object `self`.
    """

    def prefixed_method(*args, **kwargs):
        self = args[0]
        key = f(*args, **kwargs)
        return f"{self.prefix}:{key}"

    return prefixed_method


class Keys:
    """Methods to generate key names for Redis data structures."""

    def __init__(self, prefix: str = DEFAULT_KEY_PREFIX):
        self.prefix = prefix

    @prefixed_key
    def timeseries_sensor_key(self) -> str:
        """A time series containing sensor data."""
        return f"sensor:mean:5s"

    @prefixed_key
    def cache_key(self) -> str:
        return f"cache"


async def persist(keys: Keys, data: Dict[str, int]):
    ts_sensor_key = keys.timeseries_sensor_key()
    data = json.loads(data)
    dt = datetime.strptime(data['datetime'], '%Y-%m-%dT%H:%M:%S.%f')
    timestamp = int(float(datetime.timestamp(dt)) * 1000)
    return await redis.execute_command('TS.ADD', ts_sensor_key, timestamp, data['payload'])



async def get_five_sec_average(ts_key: str, top_of_five_sec: int):
    response = await redis.execute_command(
        "TS.RANGE", ts_key, top_of_five_sec, "+", "AGGREGATION", "avg", FIVE_SEC_BUCKET
    )

    return response


def datetime_parser(dct):
    for k, v in dct.items():
        if isinstance(v, str) and v.endswith("+00:00"):
            try:
                dct[k] = datetime.datetime.fromisoformat(v)
            except:
                pass
    return dct


async def get_cache(keys: Keys):
    current_five_sec_cache_key = keys.cache_key()
    current_five_sec_stats = await redis.get(current_five_sec_cache_key)

    if current_five_sec_stats:
        return json.loads(current_five_sec_stats, object_hook=datetime_parser)


@app.post("/sensor/")
async def create_item(
    item: SensorItem, background_tasks: BackgroundTasks, keys: Keys = Depends(make_keys)
):
    await persist(keys, item.json())
    

@app.websocket("/manipulator/")
async def get_sum_data(websocket: WebSocket, keys: Keys = Depends(make_keys)):
    await websocket.accept()
    while True:
        dt = datetime.now()
        five_sec_ms = int((datetime.now() - timedelta(seconds=5)).timestamp() * 1000)
        response = await redis.execute_command(
            'TS.RANGE', keys.timeseries_sensor_key(), five_sec_ms, '+', 'AGGREGATION', 'avg', FIVE_SEC_BUCKET
        )

        value = float(response[0][1])
        status = 'down' if value < 50. else 'up'
        data = json.dumps({'datetime': str(dt), 'status': status})
            
        await websocket.send_text(str(data))
        # await asyncio.sleep(5)
        await asyncio.sleep(1)
    


async def make_timeseries(key):
    try:
        await redis.execute_command(
            "TS.CREATE",
            key,
            "DUPLICATE_POLICY",
            "first",
        )
    except ResponseError as e:
        # Time series probably already exists
        log.info("Could not create timeseries %s, error: %s", key, e)


async def initialize_redis(keys: Keys):
    await make_timeseries(keys.timeseries_sensor_key())


@app.on_event("startup")
async def startup_event():
    keys = Keys()
    await initialize_redis(keys)
