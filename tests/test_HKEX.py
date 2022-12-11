from _exchanges import HKEX
import pandas as pd
import random
import requests
from io import StringIO


### HKEX
def test_aggregated_short():
    csv_link = HKEX.aggregated_short_directory()['CSV Link'][random.randint(0,10)]
    r = requests.get(csv_link, headers={"User-Agent": "Foo Bar"})
    scraped_df = pd.read_csv(StringIO(r.text))
    scraped_df["Date"] = pd.to_datetime(scraped_df["Date"], format="%d/%m/%Y")
    assert len(scraped_df.index) > 100

def test_shortable_list():
    l = HKEX.shortable_tickers()
    assert len(l) > 100

def test_securities_details():
    df = HKEX.securities_details()
    assert len(df.index) > 100