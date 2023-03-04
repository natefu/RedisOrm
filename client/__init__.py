from .redis_client import RedisClient
import json
from pathlib import Path

with open(f'{Path(__file__).parent.parent}/config.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
    conn = RedisClient(**data)

