import json
from sqlite3.dbapi2 import Connection
import requests
from requests.models import Response
import sqlite3
from discord import Webhook, RequestsWebhookAdapter

# To check consistency.
API_KEYS = {'maxSupply', 'isAudited', 'totalSupply',
            'dateAdded', 'auditInfoList', 'slug', 'cmcRank',
            'name', 'isActive', 'symbol', 'lastUpdated',
            'quotes', 'marketPairCount', 'circulatingSupply',
            'id', 'platform', 'tags'}

URL = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?limit=100"
con: Connection = sqlite3.connect('example.db')


def parse_response(res: Response) -> tuple[dict, list[dict]]:
    rjson = res.json()
    status: dict = rjson["status"]
    currencies: list[dict] = rjson["data"]['cryptoCurrencyList']
    # count: int = data['totalCount']
    return status, currencies

def pipeline():

    # Get data
    r: Response = requests.get(URL)
    r.raise_for_status()

    # Extract data
    try:
        status, currencies = parse_response(r)
    except KeyError as key_err:
        raise key_err
    except json.JSONDecodeError as json_err:
        raise json_err

def setup(conn: Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS quotes (
            coin_id int, 
            coin text, 
            currency text, 
            price real, 
            volume real, 
            mc real, 
            chg1h real, 
            chg24h real, 
            chg7d real, 
            ytdchg real, 
            lastupdated text, 
            PRIMARY KEY (coin_id, lastupdated))
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS coins (
            id int PRIMARY KEY, 
            coin_id int UNIQUE, 
            name text, 
            slug text, 
            symbol text, 
            date_added text)
    """)


if __name__ == "__main__":
    setup(conn=con)
    pipeline()
