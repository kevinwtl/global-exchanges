from datetime import date, datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
import sqlite3
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from ratelimit import limits, sleep_and_retry
import numpy as np
from tqdm import tqdm


class DataScraper:
    """Get the static list of all shareholders at the specific settle date. Do not perform and calculations or filtering.

    A few things to note when using the data:
    1. Be careful with brokers who sold out
    2. Settlent Date is Trade + 2
    3. Weekends are not dropped
    """

    def __init__(self, database_path: str, days_to_scrape_data: int = 3, ticker_list: list[int] = None):
        self.days_to_scrape_data = days_to_scrape_data
        self.ticker_list = ticker_list
        self.db_connection = sqlite3.connect(database_path, check_same_thread=False)
        self.lock = Lock()
        self.pool = ThreadPoolExecutor()

    def stock_dict(self, search_date) -> dict[int, str]:
        df = pd.DataFrame(requests.get(f'https://www3.hkexnews.hk/sdw/search/stocklist.aspx?sortby=stockcode&shareholdingdate={search_date.strftime("%Y%m%d")}', timeout=5).json())
        df['c'] = pd.to_numeric(df['c'])
        df = df.set_index('c')['n']
        return df.to_dict()

    def run(self):
        for i in tqdm(range(self.days_to_scrape_data)):
            search_date = date.today() - timedelta(1) - timedelta(i)
            ticker_list = list(self.stock_dict(search_date=search_date).keys()) if self.ticker_list is None else self.ticker_list
            for ticker in tqdm(ticker_list):
                self.pool.submit(self.scrape(ticker=ticker, search_date=search_date))

    @sleep_and_retry
    @limits(calls=3, period=1)
    def scrape(self, ticker: int, search_date: datetime, temp_ticker: int = None):
        url = "https://www3.hkexnews.hk/sdw/search/searchsdw.aspx"
        payload = {"__EVENTTARGET": "btnSearch", "txtShareholdingDate": search_date.strftime("%Y/%m/%d"), "txtStockCode": str(temp_ticker if temp_ticker is not None else ticker).zfill(5)}

        try:
            response = requests.post(url, data=payload, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36"}, timeout=100)
            if response.status_code != 200:
                raise ValueError(f"""{response}. Try reducing the frequency of scraping.""")
            settlement_date = datetime.strptime(BeautifulSoup(response.content, "lxml").find(id="txtShareholdingDate")["value"], "%Y/%m/%d").date()

            # Summary Data
            summary_soup = BeautifulSoup(response.content, "lxml")
            issued_shares = int(summary_soup.find(class_="summary-value").text.replace(",", ""))
            summary_dict = {"Ticker": ticker, "Settlement Date": settlement_date, "Shares Issued": issued_shares}  # fill initial value of summary dict
            for i in range(len(summary_soup.find_all(class_="ccass-search-datarow"))):
                key = summary_soup.find_all(class_="ccass-search-datarow")[i].find_all(class_="summary-category")[0].text.strip()
                value = int(summary_soup.find_all(class_="ccass-search-datarow")[i].find(class_="value").text.replace(",", ""))
                summary_dict[key] = value
            scraped_summary_df = pd.DataFrame(summary_dict, index=[0])
            scraped_summary_df["Settlement Date"] = pd.to_datetime(scraped_summary_df["Settlement Date"]).dt.date

            # Shareholdings Data
            scraped_temp_df = pd.read_html(response.content)[0].rename(
                columns={
                    "Participant ID": "CCASS ID",
                    "Name of CCASS Participant(* for Consenting Investor Participants )": "Participant",
                    "Shareholding": "End of Day Shareholding",
                    "% of the total number of Issued Shares/ Warrants/ Units": "End of Day Shareholding (% of Issued Shares)"
                    }
            )
            scraped_temp_df = scraped_temp_df.applymap(lambda x: x.split(":")[-1].strip())
            scraped_temp_df = scraped_temp_df.replace("", np.nan)

            scraped_temp_df["Ticker"] = ticker
            scraped_temp_df["Settlement Date"] = settlement_date
            scraped_temp_df["End of Day Shareholding"] = pd.to_numeric(scraped_temp_df["End of Day Shareholding"].str.replace(",", ""))
            scraped_temp_df["End of Day Shareholding (% of Issued Shares)"] = scraped_temp_df["End of Day Shareholding"] / issued_shares
            scraped_temp_df = scraped_temp_df[["Ticker", "CCASS ID", "Settlement Date", "End of Day Shareholding", "End of Day Shareholding (% of Issued Shares)"]]

            with self.lock:
                scraped_summary_df.to_sql(name="summary", con=self.db_connection, if_exists="append", index=False)
                scraped_temp_df.to_sql(name="broker", con=self.db_connection, if_exists="append", index=False)

        except ValueError as e:
            if "No tables found" in e.args:
                # Possibly new temporary ticker due to Share Split
                stock_dict = self.stock_dict(search_date=search_date)
                original_stock_name = stock_dict[ticker]
                for temp_stock_code, temp_stock_name in stock_dict.items():
                    if temp_stock_name == f"{original_stock_name}-TEMPORARY COUNTER":
                        print(f"{ticker} @ {search_date.strftime('%Y/%m/%d')} found temporary ticker {temp_stock_code}")
                        self.scrape(ticker = ticker, search_date=search_date, temp_ticker = temp_stock_code)
                        break
            else:
                print(f"{ticker} @ {search_date.strftime('%Y/%m/%d')} encountered {e.args}")


if __name__ == "__main__":
    DataScraper(r"CCASS.db", days_to_scrape_data=5, ticker_list=[2060]).run()