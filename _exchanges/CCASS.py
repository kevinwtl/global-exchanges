from datetime import date, datetime, timedelta
import pandas as pd
import requests
from bs4 import BeautifulSoup
from _exchanges import HKEX
import random
import time
import sqlite3

database_path = r"H:\GitHub\csam-daily-reports\res\CCASS_database.pkl"
summary_database_path = r"H:\GitHub\csam-daily-reports\res\CCASS_summary.csv"
watchlist_path = r"H:\GitHub\csam-daily-reports\res\watchlist.csv"



class DataScraper:
    """Get the static list of all shareholders at the specific settle date. Do not perform and calculations or filtering.

    A few things to note when using the data:
    1. Be careful with brokers who sold out
    2. Settlent Date is Trade + 2
    3. Weekends are not dropped
    """
    def __init__(self, days_to_scrape_data: int = 3):
        self.days_to_scrape_data = days_to_scrape_data
        self.db_connection = sqlite3.connect('test_database.db')

    def run(self):
        for i in range(self.days_to_scrape_data):
            search_date = date.today() - timedelta(1) - timedelta(i)
            stock_list = HKEX.CCASS_stocks_list(search_date = search_date)
            for ticker in stock_list:
                self.scrape(ticker=ticker,search_date = search_date)
                print(f'{ticker} done!')

    def scrape(self,ticker,search_date):
        url = "https://www3.hkexnews.hk/sdw/search/searchsdw.aspx"

        payload = {"__EVENTTARGET": "btnSearch", "txtShareholdingDate": search_date.strftime("%Y/%m/%d"), "txtStockCode": str(ticker).zfill(5)}

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
            scraped_summary_df.to_sql(name=f'{ticker}_summary', con=self.db_connection, if_exists='append', index = False)

            # Shareholdings Data
            scraped_temp_df = pd.read_html(response.content)[0]
            scraped_temp_df = scraped_temp_df.applymap(lambda x: x.split(": ")[-1])
            scraped_temp_df = scraped_temp_df.rename(
                columns={
                    "Participant ID": "CCASS ID",
                    "Name of CCASS Participant(* for Consenting Investor Participants )": "Participant",
                    "Shareholding": "End of Day Shareholding",
                    "% of the total number of Issued Shares/ Warrants/ Units": "End of Day Shareholding (% of Issued Shares)"
                    }
            )
            scraped_temp_df["Ticker"] = ticker
            scraped_temp_df["Settlement Date"] = settlement_date
            scraped_temp_df["End of Day Shareholding"] = pd.to_numeric(scraped_temp_df["End of Day Shareholding"].str.replace(",", ""))
            scraped_temp_df["End of Day Shareholding (% of Issued Shares)"] = scraped_temp_df["End of Day Shareholding"] / issued_shares
            scraped_temp_df = scraped_temp_df[["Ticker", "CCASS ID", "Settlement Date", "End of Day Shareholding", "End of Day Shareholding (% of Issued Shares)"]]
            scraped_temp_df.to_sql(name=f'{ticker}', con=self.db_connection, if_exists='append', index = False)

        except Exception as e:
            print(f"{ticker} @ {search_date.strftime('%Y/%m/%d')} encountered {e.args}")




class _updater:
    tickers = pd.read_csv(watchlist_path)["Ticker"].to_list()  # FIXME: do not use class constants

    def __init__(self, tickers: list[int] = None, days_to_scrape_data: int = 3):
        # Define class attributes
        self.scraped_df = None
        self.scraped_summary_df = None
        self.tickers = tickers if tickers is not None else self.tickers  # FIXME: do not use class constants
        self.days_to_scrape_data = days_to_scrape_data
        self.participants_dict = HKEX.CCASS_participants_dict()
        self.securities_dict = HKEX.securities_name_dict()

        # Import databases
        self.database = self.load_database()
        self.summary_database = self.load_summary_database()

    def run(self):
        # Merge the newly scaped df to existing database
        self.scraped_df, self.scraped_summary_df = self.scrape_multi_pages(tickers=self.tickers, days_to_scrape_data=self.days_to_scrape_data)
        self.scraped_df = self.fill_broker_empty_rows(self.scraped_df)
        self.database = pd.concat([self.database, self.scraped_df], ignore_index=True, sort=True).dropna(subset=["CCASS ID"]).drop_duplicates(subset=["Ticker", "CCASS ID", "Settlement Date"]).reset_index(drop=True)
        self.summary_database = pd.concat([self.summary_database, self.scraped_summary_df], ignore_index=True)

        # Database Processing
        self.database = self.database[~pd.to_datetime(self.database["Settlement Date"]).dt.dayofweek.isin([5, 6])]
        self.database = self.calculate_share_change(database=self.database)

        # Export
        self.save_database()

    def single_page_query(self, ticker: int, search_date: date = date.today()) -> list[pd.DataFrame]:
        url = "https://www3.hkexnews.hk/sdw/search/searchsdw.aspx"

        payload = {"__EVENTTARGET": "btnSearch", "txtShareholdingDate": search_date.strftime("%Y/%m/%d"), "txtStockCode": str(ticker).zfill(5)}

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
            scraped_temp_df = pd.read_html(response.content)[0]
            scraped_temp_df = scraped_temp_df.applymap(lambda x: x.split(": ")[-1])
            scraped_temp_df = scraped_temp_df.rename(
                columns={
                    "Participant ID": "CCASS ID",
                    "Name of CCASS Participant(* for Consenting Investor Participants )": "Participant",
                    "Shareholding": "End of Day Shareholding",
                    "% of the total number of Issued Shares/ Warrants/ Units": "End of Day Shareholding (% of Issued Shares)"
                    }
            )
            scraped_temp_df["Ticker"] = ticker
            scraped_temp_df["Settlement Date"] = settlement_date
            scraped_temp_df["End of Day Shareholding"] = pd.to_numeric(scraped_temp_df["End of Day Shareholding"].str.replace(",", ""))
            scraped_temp_df["End of Day Shareholding (% of Issued Shares)"] = scraped_temp_df["End of Day Shareholding"] / issued_shares
            scraped_temp_df = scraped_temp_df[["Ticker", "CCASS ID", "Settlement Date", "End of Day Shareholding", "End of Day Shareholding (% of Issued Shares)"]]

            # Completed
            return [scraped_temp_df, scraped_summary_df]
        except Exception as e:
            print(f"{ticker} @ {search_date.strftime('%Y/%m/%d')} encountered {e.args}")
            return [None, None]

    def scrape_multi_pages(self, tickers: list[int], days_to_scrape_data: int) -> list[pd.DataFrame]:
        singel_page_dfs = []
        single_page_summary_dfs = []

        # Defining the Loop
        for ticker in tickers:
            for i in range(days_to_scrape_data):
                search_date = date.today() - timedelta(1) - timedelta(i)

                single_page_df, single_page_summary_df = self.single_page_query(ticker, search_date)
                singel_page_dfs.append(single_page_df)
                single_page_summary_dfs.append(single_page_summary_df)

                time.sleep(random.uniform(0.5, 1.5))

        # Returning the 2 DataFrames
        scraped_df = pd.concat(singel_page_dfs, ignore_index=True)
        scraped_summary_df = pd.concat(single_page_summary_dfs, ignore_index=True)

        return [scraped_df, scraped_summary_df]

    def calculate_share_change(self, database: pd.DataFrame):
        """Add a column of DoD Changes (in shareholding) to the DataFrame (CCASS database)."""

        # Change the data to ascending order and calculate difference
        database = database.dropna(subset=["CCASS ID"]).drop_duplicates(subset=["Ticker", "CCASS ID", "Settlement Date"]).sort_values(["Ticker", "CCASS ID", "Settlement Date"], ascending=[True, True, True]).reset_index(drop=True)
        database["DoD Share Change"] = database.groupby(["Ticker", "CCASS ID"])["End of Day Shareholding"].aggregate("diff")
        database["DoD Share Change (% of Issued Shares)"] = database.groupby(["Ticker", "CCASS ID"])["End of Day Shareholding (% of Issued Shares)"].aggregate("diff")

        return database

    @classmethod
    def fill_broker_empty_rows(cls, df: pd.DataFrame) -> pd.DataFrame:
        """If a broker sold out / initated positions (i.e. record not seen on previous / next day) -> Add a row to the df so that the movement can be calculated."""

        # Handle Sold Out Accounts (Adding an empty row back)
        date_list = sorted(list(df["Settlement Date"].unique()), reverse=True)
        for index, group in df.groupby(["Ticker", "CCASS ID"]):
            for next_date_ix, next_date in enumerate(date_list[:-1]):
                prev_date = date_list[next_date_ix + 1]
                has_pos_in_prev_day = prev_date in group["Settlement Date"].values
                has_pos_in_next_day = next_date in group["Settlement Date"].values
                is_sold_out = has_pos_in_prev_day and not has_pos_in_next_day and group[group["Settlement Date"] == prev_date].iloc[0]["End of Day Shareholding"] != 0
                is_initating_position = not has_pos_in_prev_day and has_pos_in_next_day and group[group["Settlement Date"] == next_date].iloc[0]["End of Day Shareholding"] != 0

                if is_sold_out:
                    ticker, CCASS_id = index
                    df = pd.concat([df, pd.DataFrame({"Ticker": ticker, "CCASS ID": CCASS_id, "Settlement Date": next_date, "End of Day Shareholding": 0, "End of Day Shareholding (% of Issued Shares)": 0}, index=[0])], ignore_index=True)
                elif is_initating_position:
                    ticker, CCASS_id = index
                    df = pd.concat([df, pd.DataFrame({"Ticker": ticker, "CCASS ID": CCASS_id, "Settlement Date": prev_date, "End of Day Shareholding": 0, "End of Day Shareholding (% of Issued Shares)": 0}, index=[0])], ignore_index=True)

        df = df.sort_values(["Ticker", "CCASS ID", "Settlement Date"], ascending=[True, True, False]).reset_index(drop=True)
        return df

    def sort_database(self, database: pd.DataFrame):
        database = database.sort_values(["Ticker", "CCASS ID", "Settlement Date"], ascending=[True, True, False]).reset_index(drop=True)
        return database

    def load_database(self) -> pd.DataFrame:
        try:
            database = pd.read_pickle(database_path)
            database["Settlement Date"] = pd.to_datetime(database["Settlement Date"]).dt.date  # Ensure the column is datetime.date
        except FileNotFoundError:
            print("Existing Database file (.pkl) not found. Running the remaining codes will generate a new database file.")

        return database

    def load_summary_database(self) -> pd.DataFrame:
        try:
            summary_database = pd.read_csv(summary_database_path, parse_dates=["Settlement Date"])
            summary_database["Settlement Date"] = pd.to_datetime(summary_database["Settlement Date"]).dt.date  # Ensure the column is datetime.date
        except FileNotFoundError:
            print("Existing Summary Database file (.csv) not found. Running the remaining codes will generate a new database file.")
        return summary_database

    def save_database(self):
        # Clean up the df before saving
        self.database = (
            self.database.dropna(subset=["CCASS ID"])
            .drop_duplicates(subset=["Ticker", "CCASS ID", "Settlement Date"])
            .sort_values(["Ticker", "Settlement Date", "CCASS ID"], ascending=[True, False, True])
            .reset_index(drop=True)[["Ticker", "CCASS ID", "Settlement Date", "End of Day Shareholding", "End of Day Shareholding (% of Issued Shares)", "DoD Share Change (% of Issued Shares)", "DoD Share Change"]]
        )

        self.summary_database = self.summary_database.drop_duplicates(subset=["Ticker", "Settlement Date"]).sort_values(by=["Ticker", "Settlement Date"], ascending=[True, False], ignore_index=True).reset_index(drop=True)

        # Export as Pickle & csv
        self.database.to_pickle(database_path)
        self.summary_database.to_csv(summary_database_path, index=False)


if __name__ == "__main__":
    x = DataScraper()
    x.run()
