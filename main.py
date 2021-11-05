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


# Send alerts to discord channel
webhook: Webhook = Webhook.from_url(
    DISCORD_GENERAL, adapter=RequestsWebhookAdapter())

URL = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?limit=100"
con: Connection = sqlite3.connect('specter.db')


def parse_response(res: Response) -> tuple[dict, list[dict]]:
    rjson = res.json()
    status: dict = rjson["status"]
    currencies: list[dict] = rjson["data"]['cryptoCurrencyList']
    # count: int = data['totalCount'] # Do we want to keep track of this?
    return status, currencies


def serialize_data(currencies: list[dict]) -> tuple[list, list]:
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
        webhook.send("Key Error when parsing API. Check new api fields.")
        raise key_err
    except json.JSONDecodeError as json_err:
        webhook.send("JSON Decoding error when parsing API response")
        raise json_err

    # Validate (Not implemented) - right types, right size, expected data...
    # currencies = validate(currencies)

    # Serialize data for storage
    coins, quotes = serialize_data(currencies=currencies)

    # Storage
    try:
        store(conn=con, coins=coins, quotes=quotes)
    except sqlite3.IntegrityError:
        # API caches responses for 1-3 minutes. Integrity error will be raised if same timestamp + coin_id is already stored for the quotes
        # We could tigthen or release our constraints here too. Ideally this would be would catch this earlier. 
        # Just passing for DEMO purposes
        print("Err: Received duplicated data. Data not being stored")
        pass


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
