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


DISCORD_GENERAL = ""
webhook: Webhook = Webhook.from_url(
    DISCORD_GENERAL, adapter=RequestsWebhookAdapter())

URL = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?limit=100"
con: Connection = sqlite3.connect('example.db')


def parse_response(res: Response) -> tuple[dict, list[dict]]:
    rjson = res.json()
    status: dict = rjson["status"]
    currencies: list[dict] = rjson["data"]['cryptoCurrencyList']
    # count: int = data['totalCount']
    return status, currencies


def aggregate_data(currencies: list[dict]) -> tuple[list, list]:
    coins: list[tuple] = []
    quotes: list[tuple] = []
    for cur in currencies:
        coins.append((cur["id"], cur["name"], cur["slug"],
                     cur["symbol"], cur["dateAdded"]))

        for quote in cur["quotes"]:  # Multiple currency quotes
            quotes.append((cur["id"], cur["name"], quote["name"], quote["price"],
                          quote["volume24h"], quote["marketCap"], quote["percentChange1h"],
                          quote["percentChange24h"], quote["percentChange7d"],
                          quote["ytdPriceChangePercentage"], quote["lastUpdated"]))
    return coins, quotes


def store(conn: Connection, coins: list, quotes: list):
    # COINS
    conn.executemany(
        """
        INSERT OR IGNORE INTO coins (coin_id, name, slug, symbol, date_added) 
        VALUES (?,?,?,?,?)
        """, coins)
    conn.commit()

    # QUOTES
    conn.executemany(
        """
        INSERT INTO quotes (coin_id, coin, currency, price, volume, mc, chg1h, chg24h, chg7d, ytdchg, lastupdated) 
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, quotes)
    conn.commit()


def pipeline():

    # Get data
    r: Response = requests.get(URL)
    r.raise_for_status()

    # Extract data
    try:
        status, currencies = parse_response(r)
    except KeyError as key_err:
        webhook.send("Key Error when parsing API. Check new api keys.")
        raise key_err
    except json.JSONDecodeError as json_err:
        webhook.send("JSON Decoding error when parsing API")
        raise json_err

    # Validate - right types, right size, expected data...
    # currencies = validate(currencies)

    # Aggregate
    coins, quotes = aggregate_data(currencies=currencies)

    # Storage
    store(conn=con, coins=coins, quotes=quotes)


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
