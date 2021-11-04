import json
from sqlite3.dbapi2 import Connection
import requests
from requests.models import Response
import sqlite3
from discord import Webhook, RequestsWebhookAdapter

URL = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?limit=100"
con: Connection = sqlite3.connect('example.db')

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
