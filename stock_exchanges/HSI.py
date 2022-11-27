import pandas as pd
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import requests
import json
import camelot

class HSI:
    @property
    def index_code_dict(self) -> dict:
        """Dictionary mapping `HSI index code` and the `Full Index Name`.

        Returns:
            dict: {`index_code` (str): `index_full_name` (str)}
        """

        browser = webdriver.Chrome(ChromeDriverManager().install()) # Download chromdriver
        browser.minimize_window()
        browser.get(r'https://www.hsi.com.hk/eng/indexes/all-indexes')
        soup = BeautifulSoup(browser.page_source, 'lxml')

        d = {}
        for brick in soup.find_all('masonry-brick'):
            for row in brick.find_all('a'):
                index_code = row['href'].split('/')[-1]
                index_full_name = row.text
                d[index_code] = index_full_name
        return d

    @staticmethod
    def index_constituents(index_code: str) -> pd.DataFrame:
        """Return a DataFrame summarizing all the constituents in the Index.

        Returns:
            pd.DataFrame:
                Index:
                    RangeIndex
                Column:
                    Stock Code: int32
                    Stock Name: object
                    Share Type: object
                    Stock Code (A): object
                    Stock Code (B): object
                    Stock Code (H): object
                    Stock Code (R): object
                    Stock Code (O): object
                    Stock Code (T): object
                    Change Contribution: int64
                    Industry: object
                    Industry Code: object
                    Index Name: object
                    Index Code: object
        """
        # Getting the data
        index_code = index_code.lower()
        r = requests.request("GET", f"https://www.hsi.com.hk/data/eng/rt/index-series/{index_code}/constituents.do")
        json_data = json.loads(r.content)['indexSeriesList'][0]
        df = pd.json_normalize(json_data['indexList'][0]['constituentContent'])

        # Marking down the Index Name & inferring dtypes
        df['seriesName'] = json_data['seriesName']
        df['seriesCode'] = json_data['seriesCode']
        df['code'] = df['code'].astype(int)
        df['contributionChange'] = df['contributionChange'].astype(int)

        # Rename columns
        df.columns = ['Stock Code', 'Stock Name', 'Share Type', 'Stock Code (A)', 'Stock Code (B)', 'Stock Code (H)', 'Stock Code (R)','Stock Code (O)', 'Stock Code (T)', 'Change Contribution', 'Industry', 'Industry Code','Index Name', 'Index Code']

        return df

    @staticmethod
    def get_constituent_weight(stock_code: int, index_code: str) -> float:
        try:
            page_number = 4 if index_code == 'hsci' else 2
            tables = camelot.read_pdf(f'https://www.hsi.com.hk/static/uploads/contents/en/dl_centre/factsheets/{index_code}e.pdf',flavor='stream',strip_text='\n',pages=str(page_number))
            df = tables[0].df
            df.columns = df.iloc[1]
            df = df.iloc[2:].reset_index(drop=True)
            df.columns.name = None
            df['Index Code'] = index_code

            # Dropping nan
            df = df.replace('',pd.NaT).dropna(thresh=3)
            df = df.dropna(axis= 0, thresh = 5)
            df = df.dropna(axis= 1, thresh = 10)

            # Infer dtypes
            df['Stock Code'] = df['Stock Code'].astype(int)
            df['Weighting (%)'] = df['Weighting (%)'].astype(float)

            weight = df[df['Stock Code'] == stock_code].iloc[0]['Weighting (%)']
        except Exception:
            weight = ''

        return weight

    @classmethod
    def all_indexes_of_security(cls, stock_code:int):
        index_code_list = list(set(cls().index_code_dict.keys()))

        # Going through all the indexes
        dfs = []
        for index_code in index_code_list:
            try:
                dfs.append(cls().index_constituents(index_code))
            except Exception as e:
                print(f"{e} when requesting {index_code}")
        df = pd.concat(dfs,ignore_index=True)

        # Filtering for our targetted stock
        df = df[df['Stock Code'] == stock_code].reset_index(drop=True)

        # Find Constituent Weighting
        df['Weighting (%)'] = df['Index Code'].apply(lambda x: cls().get_constituent_weight(stock_code, x))

        # Remove unwanted columns and reorder columns
        df = df[['Stock Code','Stock Name','Index Code','Index Name','Weighting (%)','Change Contribution']]

        return df
