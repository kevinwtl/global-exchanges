import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from io import StringIO
from datetime import datetime, timedelta, date
import json
from typing import Literal
import warnings

from _exchanges import CCASS


class HKEX:
    @staticmethod
    def shortable_tickers() -> list:
        """Download the List of Shortable Securities from HKEX. Downloaded from https://www.hkex.com.hk/Services/Trading/Securities/Securities-Lists/Designated-Securities-Eligible-for-Short-Selling.

        Returns:
        ---
            list: A list of integers.
        """
        # Download the latest list of Shortable Securities
        r = requests.get(r"https://www.hkex.com.hk/Services/Trading/Securities/Securities-Lists/Designated-Securities-Eligible-for-Short-Selling?sc_lang=en")
        soup = BeautifulSoup(r.content, features="lxml")
        query = soup.find(attrs="table").find_all("tr")[1].find_all("td")[2].find_all("a")[0]["href"]
        url = f"https://www.hkex.com.hk{query}"
        ticker_list = pd.read_html(url, header=0, index_col=0)[0].reset_index(drop=True)["Stock Code"].to_list()
        return ticker_list

    @staticmethod
    def aggregated_short_directory() -> pd.DataFrame:
        """Download the list of Aggregated Reportable Short Positions Reports. Includes report date and links to the reports. Downloaded from https://www.sfc.hk/en/Regulatory-functions/Market/Short-position-reporting/Aggregated-reportable-short-positions-of-specified-shares/.

        Returns:
        ---
            pd.DataFrame:
                Index:
                    RangeIndex
                Columns:
                    Reporting Date: datetime64[ns]
                    PDF Link: object
                    CSV Link: object
        """
        r = requests.get(r"https://www.sfc.hk/en/Regulatory-functions/Market/Short-position-reporting/Aggregated-reportable-short-positions-of-specified-shares/", headers={"User-Agent": "Foo Bar"})
        soup = BeautifulSoup(r.content, features="lxml")

        trs = []
        for row in soup.find("table").find_all("tr")[1:]:
            report_date = row.find_all("td")[0].text
            pdf_link = row.find_all("td")[1].a["href"].split('?')[0]
            csv_link = row.find_all("td")[2].a["href"].split('?')[0]
            trs.append([report_date, pdf_link, csv_link])

        df = pd.DataFrame(trs, columns=["Reporting Date", "PDF Link", "CSV Link"])
        df["Reporting Date"] = pd.to_datetime(df["Reporting Date"])

        return df

    @classmethod
    def aggregated_short_latest_report(cls) -> pd.DataFrame:
        """Returns a DataFrame containing the latestest Aggregated Reportable Short Positions Report. Downloaded from https://www.sfc.hk/en/Regulatory-functions/Market/Short-position-reporting/Aggregated-reportable-short-positions-of-specified-shares/.

        Returns:
        ---
            pd.DataFrame:
                Index:
                    RangeIndex
                Columns:
                    Date, datetime64[ns]
                    Stock Code, int64
                    Stock Name, object
                    Aggregated Reportable Short Positions (Shares), int64
                    Aggregated Reportable Short Positions (HK$), int64
        """
        directory = cls.aggregated_short_directory()
        report_link = directory["CSV Link"][0]
        r = requests.get(report_link, headers={"User-Agent": "Foo Bar"})

        df = pd.read_csv(StringIO(r.text))
        df["Date"] = pd.to_datetime(df["Date"], format="%d/%m/%Y")

        return df

    @staticmethod
    def securities_details() -> pd.DataFrame:
        """Download the latest List of Securities from HKEX website, including all the listing details. Downloaded from https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx.

        Returns:
        ---
            pd.DataFrame:
                Index:
                    RangeIndex
                Columns:
                    Stock Code: int64
                    Name of Securities: object
                    Category: object
                    Sub-Category: object
                    Board Lot: int64
                    Par Value: object
                    ISIN: object
                    Expiry Date: object
                    Subject to Stamp Duty: bool
                    Shortsell Eligible: bool
                    CAS Eligible: bool
                    VCM Eligible: bool
                    Admitted to Stock Options: bool
                    Admitted to Stock Futures: bool
                    Admitted to CCASS: bool
                    ETF / Fund Manager: object
                    Debt Securities Board Lot (Nominal): object
                    Debt Securities Investor Type: object
                    POS Eligble: bool
                    Spread Schedule: int64
        """
        df = pd.read_excel(r"https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx", skiprows=range(2), thousands=",", decimal=".", true_values="Y", false_values=" ")
        df.rename(columns={"Spread Table\n1, 4 = Part A\n3 = Part B\n5 = Part D": "Spread Schedule"}, inplace=True)
        return df

    @classmethod
    def securities_name_dict(cls) -> dict:
        """A dictionary mapping Stock Code and Stock Name. Downloaded from https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx.

        Returns:
        ---
            dict: A dict of {int: str}. Example: {1: 'CKH HOLDINGS'}
        """
        my_dict = cls.securities_details().set_index("Stock Code")["Name of Securities"].to_dict()

        return my_dict

    @staticmethod
    def CCASS_participants_dict() -> dict:
        """A dictionary mapping the CCASS ID and the corresponding Broker Name.

        Returns:
        ---
            dict: A dict of {str: str}. Example: {'B00128': 'KAM WAH SECURITIES CO'}
        """
        participants_dict = (
            pd.DataFrame(requests.get(f'https://www.hkexnews.hk/sdw/search/partlist.aspx?sortby=partid&shareholdingdate={datetime.today().strftime("%Y%m%d")}').json())
            .rename(columns={"c": "Participant ID", "n": "Participant Name"})
            .set_index("Participant ID", drop=True)["Participant Name"]
            .to_dict()
        )
        return participants_dict

    @staticmethod
    def CCASS_stocks_list(search_date: datetime = datetime.today()) -> list[int]:
        """A list returning all available stocks on CCASS at particular settlement date."""
        CCASS_stock_list = pd.DataFrame(requests.get(f'https://www3.hkexnews.hk/sdw/search/stocklist.aspx?sortby=stockcode&shareholdingdate={search_date.strftime("%Y%m%d")}').json())['c'].to_list()
        CCASS_stock_list = [int(i) for i in CCASS_stock_list]
        return CCASS_stock_list

    @staticmethod
    def southbound_eligilble_stocks() -> pd.DataFrame:
        """Get the list of southbound eligible stocks from Shanghai Stock Exchange (http://www.szse.cn/szhk/hkbussiness/underlylist/) and Shenzhen Stock Exchange (http://www.sse.com.cn/services/hkexsc/disclo/eligible/).

        Returns
        ---
            pd.DataFrame:
                Index:
                    RangeIndex
                Columns:
                    Stock Code: int64
                    SH Connect: bool
                    SZ Connect: bool
        """

        # 1. Shanghai Stock Connect
        ## Data Query
        url = "http://query.sse.com.cn/commonQuery.do"
        querystring = {"sqlId": "COMMON_SSE_JYFW_HGT_XXPL_BDZQQD_L"}
        headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6",
            "Connection": "keep-alive",
            "Cookie": "gdp_user_id=gioenc-9g2aae7g^%^2C9g27^%^2C57ae^%^2Ca1ag^%^2Cc9ga6893786d; yfx_c_g_u_id_10000042=_ck22081513144410577925234729704; ba17301551dcbaf9_gdp_session_id=ced13ec0-1ff8-4857-a9c6-f36b40ac6df7; yfx_f_l_v_t_10000042=f_t_1660540484010__r_t_1661909638785__v_t_1661909638785__r_c_1; ba17301551dcbaf9_gdp_session_id_ced13ec0-1ff8-4857-a9c6-f36b40ac6df7=true",
            "Referer": "http://www.sse.com.cn/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
        }
        r = requests.get(url, params=querystring, headers=headers)
        sh_df = pd.DataFrame(json.loads(r.content)["result"])

        ## Infer dtypes
        sh_df["SECURITY_CODE"] = sh_df["SECURITY_CODE"].astype("int64")
        sh_df["ABBR_CN"] = sh_df["ABBR_CN"].replace("\u3000", "", regex=True)
        sh_df = sh_df[["SECURITY_CODE", "ABBR_CN", "ABBR_EN"]]
        sh_df.columns = ["Stock Code", "Chinese Name", "English Name"]
        sh_df["SH Connect"] = True

        # 2.Shenzhen Stock Connect
        warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")  # Disable warnings from reading excel

        sz_df = pd.read_excel(r"http://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=SGT_GGTBDQD&TABKEY=tab1&random=0.6783722097527984", engine="openpyxl")
        sz_df.columns = ["Stock Code", "Chinese Name", "English Name"]
        sz_df["SZ Connect"] = True

        # Combining Both Exchanges
        combined_df = sh_df[["Stock Code", "SH Connect"]].merge(sz_df[["Stock Code", "SZ Connect"]], how="outer", on=["Stock Code"])
        combined_df.fillna(False, inplace=True)
        combined_df = combined_df.sort_values("Stock Code").reset_index(drop=True)

        return combined_df

    @staticmethod
    def CCASS_southbound_latest_shareholdings() -> pd.DataFrame:
        """Download the latest southbound shareholding data from HKEX website. Does not mean the list of eligible stocks. Downloaded from https://www3.hkexnews.hk/sdw/search/mutualmarket.aspx?t=hk.

        Returns:
        ---
            pd.DataFrame:
                Index:
                    RangeIndex
                Columns:
                    Stock Code: Ticker, dtype: int32
                    Name: Stock Name, dtype: object
                    Shareholding in CCASS: Abosulte Shareholding, dtype: int64
                    % of the total number of Issued Shares/Units: Percentage Holdings (as decimal), dtype: float64
        """
        df = pd.read_html(r"https://www3.hkexnews.hk/sdw/search/mutualmarket.aspx?t=hk")[1]

        # Remove Duplicated Text in cells
        df.replace("Stock Code: ", "", regex=True, inplace=True)
        df.replace("Name: ", "", regex=True, inplace=True)
        df.replace("Shareholding in CCASS: ", "", regex=True, inplace=True)
        df.replace("% of the total number of Issued Shares/Units: ", "", regex=True, inplace=True)

        # Explicitly Infer dtypes
        df["Stock Code"] = df["Stock Code"].astype(int)
        df["Shareholding in CCASS"] = df["Shareholding in CCASS"].replace(",", "", regex=True).astype("int64")
        df[r"% of the total number of Issued Shares/Units"] = df[r"% of the total number of Issued Shares/Units"].replace("%", "", regex=True).astype(float) / 100
        return df

    @staticmethod
    def backend_stockID_dict() -> dict:
        """Return a mapping dictionary that HKEX API (e.g. News and DI) uses. Following {Stock Code: API ID}

        Returns:
        ---
            dict: A dict of {int: int}. Example: {65: 35207}, meaning 0065 HK has the ID of 35207.
        """
        url = r"https://www.hkexnews.hk/ncms/script/eds/activestock_sehk_e.json"
        response = requests.get(url, data="")

        df = pd.DataFrame(json.loads(response.text))[["c", "i"]]
        df["c"] = df["c"].astype(int)
        my_dict = df.set_index("c")["i"].to_dict()
        return my_dict

    @staticmethod
    def buyback_report(dt: date = date.today()) -> pd.DataFrame:
        url = f"https://www.hkexnews.hk/reports/sharerepur/documents/SRRPT{dt.strftime('%Y%m%d')}.xls"
        df = pd.read_excel(url, sheet_name="SBNReport").dropna().replace({"\n": " "}, regex=True)
        df.columns = df.iloc[0]
        df = df[1:]
        df.columns.name = None
        df = df.reset_index(drop=True)
        return df

    @classmethod
    def company_news(cls, ticker: int, start_date: date, lang: Literal["en", "zh"] = "en") -> pd.DataFrame:
        def replace_special_characters(x: str):
            """Function to used in pd.apply(). Replacing Special Characters in file names that interrupt saving.

            Args:
                str: File Name

            Returns:
                str: File Name (without special characters)
            """
            char_dict = {"/": "-", "\n": " ", ":": "-", '"': "'", "*": "", "\t": " "}
            my_str = str(x)
            for key, value in char_dict.items():
                my_str = my_str.replace(key, value)

            return my_str

        headers = {
            "cookie": "JSESSIONID=9A2chda-vHf19pWD_MO_KTzUpMnvWF0_mPhkmkZp; TS014a5f8b=015e7ee6039e0c0ecbca757e992ee7f5975d34c994954f93e2269f2750b5147849b1e6447c2a567300fbda70b4fc82a4830f222fe5d2309b1cf39ceb4ebae15d41635d501f; TS1bd2d06d027=08754bc291ab20008c12b558fb0472615a30d7d9e0a7070a916c5ab618ef468be77f2e564c7f1ec80878b6634a1130003730d6a2e58976d1eba64a6a924215e708f22ecbf284d43886bc656a4025e9e51726d0eeb0aafd8396cd98e2cd12963f; sclang=en; TS38b16b21027=086f2721efab2000b2f90e68a739db4ddec066a1813d6f1951dae8a885243f89f0a0b74ee3dc7fb4086fbf33e0113000442b0c449878d691a70c827b0a53f48a176f3afb735b9b6769ad42e1197a5bc0cdeb76b3183f33317bea1f5006317020; TS0168982d=015e7ee603cd656c91e7eb45d9fda838f8749defa8f67f05bfeee6ac407cb3750ad14517a7017500519f542cb6f04220fca490fb6c; TS4e849b71027=08754bc291ab20002deff3e19516f55334dee85df9103c918692893e7acb4500c30b3004b2ec11c608ce87b6a71130000b5d77522cebd0f45076628a0e1b8efcccfc5771a7c8acd1781e543be59d46425c29bfbae51eeff6103b99d6c9aa40f6; WT_FPC=id=175.45.42.221-2972079472.30879593:lv=1656653237416:ss=1656653138852",
            "authority": "www1.hkexnews.hk",
            "accept": "*/*",
            "accept-language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6",
            "referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=zh",
            "sec-ch-ua": r"^\^.Not/A",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": r"^\^Windows^^",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
        }

        stockID = cls.backend_stockID_dict()[ticker]
        start_date = start_date.strftime("%Y%m%d")
        end_date = datetime.today().strftime("%Y%m%d")
        url = f"https://www1.hkexnews.hk/search/titleSearchServlet.do?sortDir=0&sortByOptions=DateTime&category=0&market=SEHK&stockId={str(stockID)}&documentType=-1&fromDate={start_date}&toDate={end_date}&title=&searchType=0&t1code=-2&t2Gcode=-2&t2code=-2&rowRange=200&lang={lang}"
        response = requests.get(url, headers=headers)
        data = json.loads(response.content.decode("utf-8"))["result"]
        df = pd.read_json(data, convert_dates=False)
        df = df[["STOCK_CODE", "DATE_TIME", "TITLE", "FILE_LINK"]]
        df["STOCK_CODE"] = df["STOCK_CODE"].astype(str) + " HK"
        df["DATE_TIME"] = pd.to_datetime(df["DATE_TIME"], format="%d/%m/%Y %H:%M")
        df["TITLE"] = df["TITLE"].str.strip().apply(str.title)  # Capitalize First Letter
        df["FILE_LINK"] = r"https://www1.hkexnews.hk/" + df["FILE_LINK"]
        df["FILE_NAME"] = df["STOCK_CODE"] + " - " + df["DATE_TIME"].dt.strftime("%Y-%m-%d") + " - " + df["TITLE"]
        df["FILE_NAME"] = df["FILE_NAME"].apply(replace_special_characters)  # File Name Cannot contain special char
        df["FILE_NAME"] = df["FILE_NAME"].apply(lambda x: (x[:140] + "..") if len(x) > 142 else x)  # File Title length limit
        df["FILE_NAME"] = df["FILE_NAME"] + ".pdf"

        df.columns = ["Stock Code", "Release Time", "File Title", "File Link", "File Name"]

        return df[["Stock Code", "Release Time", "File Title", "File Link"]]

    class DI:
        header = {
            "cookie": "JSESSIONID=9A2chda-vHf19pWD_MO_KTzUpMnvWF0_mPhkmkZp; TS014a5f8b=015e7ee6039e0c0ecbca757e992ee7f5975d34c994954f93e2269f2750b5147849b1e6447c2a567300fbda70b4fc82a4830f222fe5d2309b1cf39ceb4ebae15d41635d501f; TS1bd2d06d027=08754bc291ab20008c12b558fb0472615a30d7d9e0a7070a916c5ab618ef468be77f2e564c7f1ec80878b6634a1130003730d6a2e58976d1eba64a6a924215e708f22ecbf284d43886bc656a4025e9e51726d0eeb0aafd8396cd98e2cd12963f; sclang=en; TS38b16b21027=086f2721efab2000b2f90e68a739db4ddec066a1813d6f1951dae8a885243f89f0a0b74ee3dc7fb4086fbf33e0113000442b0c449878d691a70c827b0a53f48a176f3afb735b9b6769ad42e1197a5bc0cdeb76b3183f33317bea1f5006317020; TS0168982d=015e7ee603cd656c91e7eb45d9fda838f8749defa8f67f05bfeee6ac407cb3750ad14517a7017500519f542cb6f04220fca490fb6c; TS4e849b71027=08754bc291ab20002deff3e19516f55334dee85df9103c918692893e7acb4500c30b3004b2ec11c608ce87b6a71130000b5d77522cebd0f45076628a0e1b8efcccfc5771a7c8acd1781e543be59d46425c29bfbae51eeff6103b99d6c9aa40f6; WT_FPC=id=175.45.42.221-2972079472.30879593:lv=1656653237416:ss=1656653138852",
            "authority": "www1.hkexnews.hk",
            "accept": "*/*",
            "accept-language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6",
            "referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=zh",
            "sec-ch-ua": r"^\^.Not/A",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": r"^\^Windows^^",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
        }

        def __init__(self, sid):
            self.sid = sid

        def all_filings(self, search_day_range: int = 365):
            # Constants
            forms_dict = {"IS": "1", "CS": "2", "DA": "3A", "DB": "3B"}
            DI_reason_dict = self.DI_disclose_reason_dict()

            # Getting Data
            dt = datetime.today()
            dfs = []
            for page in range(5):
                url = f"https://di.hkex.com.hk/di/NSAllFormList.aspx?sid={self.sid}&sd={(dt-timedelta(days=search_day_range)).strftime('%d/%m/%Y')}&ed={dt.strftime('%d/%m/%Y')}&cid=0&scsd={(dt-timedelta(days=search_day_range)).strftime('%d/%m/%Y').replace('/','%2f')}&sced={dt.strftime('%d/%m/%Y').replace('/','%2f')}&&pg={page}"
                r = requests.get(url, headers=self.header)
                df = pd.read_html(r.content, header=0)[5]
                dfs.append(df)
            df = pd.concat(dfs, ignore_index=True)

            # Processing Data
            df["Reason for disclosure"] = df["Reason for disclosure"].str[:4]
            df["Reason for disclosure"] = df["Reason for disclosure"].map(DI_reason_dict)
            for col_name in ["No. of shares bought / sold / involved", "No. of shares interested (See *Notes above)", "% of issued voting shares"]:
                df[col_name] = df[col_name].str[:-3]  # .replace(',','',regex=True).astype(int)
            df["Average price per share"] = df["Average price per share"].str[4:]
            df = df[~df["Form Serial Number"].str.contains("Superseded")]
            df["Form Serial Number"] = df["Form Serial Number"].apply(lambda x: x[:ix] if (ix := x.find("(")) != -1 else x)
            df["Url"] = df["Form Serial Number"].apply(lambda x: f"""https://di.hkex.com.hk/di/NSForm{forms_dict[x[0:2]]}.aspx?fn={x}""")  # Adding Hyperlink
            df["Supplementary Information"] = df["Url"].apply(self.get_DI_supp_info)
            df["Date of relevant event (dd/mm/yyyy)"] = pd.to_datetime(df["Date of relevant event (dd/mm/yyyy)"], format="%d/%m/%Y").dt.strftime("%b %#d, %Y")

            # Renaming columns
            df = df.rename(
                columns={
                    "Form Serial Number": "Form ID",
                    "Name of substantial shareholder / director / chief executive": "Shareholder Name",
                    "Reason for disclosure": "Reason for Disclosure",
                    "No. of shares bought / sold / involved": "Shares Involved",
                    "Average price per share": "Average Price",
                    "No. of shares interested (See *Notes above)": "Total Shares Interested (After event)",
                    "% of issued voting shares": "Total % Shares Interested (After event)",
                    "Date of relevant event (dd/mm/yyyy)": "Event Date",
                    "Interests in shares of associated corporation": "Interests in Shares of Associated Corporation",
                    "Interests in debentures": "Interests in Debentures",
                }
            )

            return df

        def DI_disclose_reason_dict(self) -> dict:
            df = pd.read_html(requests.get("https://di.hkex.com.hk/di/NSStdCode.aspx?ft=DA&lang=EN", headers=self.header).content, flavor="bs4", skiprows=4)[0].dropna()
            df.columns = df.iloc[0]
            df.columns.name = None
            df.index.name = None
            df = df.iloc[1:].reset_index(drop=True)
            return df.set_index("Code")["Description"].to_dict()

        def get_DI_supp_info(self, url) -> str:
            df = pd.read_html(requests.get(url, headers=self.header).content, flavor="bs4")[0]
            try:
                row_idx = df[0][df[0].astype(str).str.contains(".  Supplementary information:")].index[-1]
                supp_info = df.iloc[row_idx, 1]
                # x,y = df[df.isin([r'24.  Supplementary information:'])].stack().index[0]
                # supp_info = df.iloc[x,y+1]
                return supp_info
            except IndexError:
                return np.nan

        @staticmethod
        def daily_summary(dt: date = date.today()) -> pd.DataFrame:
            header = {
                "cookie": "JSESSIONID=9A2chda-vHf19pWD_MO_KTzUpMnvWF0_mPhkmkZp; TS014a5f8b=015e7ee6039e0c0ecbca757e992ee7f5975d34c994954f93e2269f2750b5147849b1e6447c2a567300fbda70b4fc82a4830f222fe5d2309b1cf39ceb4ebae15d41635d501f; TS1bd2d06d027=08754bc291ab20008c12b558fb0472615a30d7d9e0a7070a916c5ab618ef468be77f2e564c7f1ec80878b6634a1130003730d6a2e58976d1eba64a6a924215e708f22ecbf284d43886bc656a4025e9e51726d0eeb0aafd8396cd98e2cd12963f; sclang=en; TS38b16b21027=086f2721efab2000b2f90e68a739db4ddec066a1813d6f1951dae8a885243f89f0a0b74ee3dc7fb4086fbf33e0113000442b0c449878d691a70c827b0a53f48a176f3afb735b9b6769ad42e1197a5bc0cdeb76b3183f33317bea1f5006317020; TS0168982d=015e7ee603cd656c91e7eb45d9fda838f8749defa8f67f05bfeee6ac407cb3750ad14517a7017500519f542cb6f04220fca490fb6c; TS4e849b71027=08754bc291ab20002deff3e19516f55334dee85df9103c918692893e7acb4500c30b3004b2ec11c608ce87b6a71130000b5d77522cebd0f45076628a0e1b8efcccfc5771a7c8acd1781e543be59d46425c29bfbae51eeff6103b99d6c9aa40f6; WT_FPC=id=175.45.42.221-2972079472.30879593:lv=1656653237416:ss=1656653138852",
                "authority": "www1.hkexnews.hk",
                "accept": "*/*",
                "accept-language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,zh-CN;q=0.6",
                "referer": "https://www1.hkexnews.hk/search/titlesearch.xhtml?lang=zh",
                "sec-ch-ua": r"^\^.Not/A",
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": r"^\^Windows^^",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
                "x-requested-with": "XMLHttpRequest",
            }

            def txt_transform(txt):
                if type(txt) == str:
                    txt_list = txt.replace(")", ") ").split(" ")
                    for i, subtext in enumerate(txt_list):
                        idx = subtext.find(".")
                        if idx != -1:  # i.e. The dot exists
                            txt_list[i] = [subtext[: idx + 3], subtext[idx + 3 :]]
                        else:
                            txt_list[i] = [subtext]
                    txt_list = [x for xs in txt_list for x in xs]  # flatten list
                    txt_list = list(filter(None, txt_list))  # remove empty strings
                    return "; ".join(txt_list[::2])

            def txt_transform_pct(txt):
                if type(txt) == str:
                    txt_list = txt.replace(")", ") ").split(" ")
                    for i, subtext in enumerate(txt_list):
                        idx = subtext.find(".")
                        if idx != -1:  # i.e. The dot exists
                            txt_list[i] = [subtext[: idx + 3], subtext[idx + 3 :]]
                        else:
                            txt_list[i] = [subtext]
                    txt_list = [x for xs in txt_list for x in xs]  # flatten list
                    txt_list = list(filter(None, txt_list))  # remove empty strings
                    return "; ".join(txt_list[1::2])

            dfs = []
            columns_header_dict = {
                1: [
                    "Serial No",
                    "Name of listed corporation",
                    "Stock code",
                    "Class of shares",
                    "Name of substantial shareholder",
                    "Date of relevant event",
                    "Reason for disclosure",
                    "Number of shares bought / sold / involved",
                    "Average price per share",
                    "Previous No. of shares",
                    "Previous No. of shares (%)",
                    "Present No. of shares",
                    "Present No. of shares (%)",
                ],
                2: [
                    "Serial No",
                    "Name of listed corporation",
                    "Stock code",
                    "Class of shares",
                    "Name of substantial shareholder",
                    "Date of relevant event",
                    "Reason for disclosure",
                    "Number of shares bought / sold / involved",
                    "Average price per share",
                    "Previous No. of shares",
                    "Previous No. of shares (%)",
                    "Present No. of shares",
                    "Present No. of shares (%)",
                ],
                3: [
                    "Serial No",
                    "Name of listed corporation",
                    "Stock code",
                    "Class of shares",
                    "Name of substantial shareholder",
                    "Date of relevant event",
                    "Reason for disclosure",
                    "Number of shares bought / sold / involved",
                    "Average price per share",
                    "Previous No. of shares",
                    "Previous No. of shares (%)",
                    "Present No. of shares",
                    "Present No. of shares (%)",
                ],
                4: [
                    "Serial No",
                    "Name of listed corporation",
                    "Stock code",
                    "Associated Corporation",
                    "Name of director",
                    "Date of relevant event",
                    "Reason for disclosure",
                    "Currency of debentures",
                    "Amount of debentures bought / sold / involved",
                    "Average price per unit",
                    "Previous Balance",
                    "Present Balance",
                ],
            }

            for i in range(1, 5):
                try:
                    url = f'https://di.hkex.com.hk/di/summary/DSM{dt.strftime("%Y%m%d")}C{str(i)}.htm'
                    r = requests.get(url, headers=header)
                    table = BeautifulSoup(r.content, "lxml").find(id="Table3")
                    df = pd.read_html(str(table), header=1)[0]
                    df = df.dropna(axis=0, how="all")
                    df.columns = columns_header_dict[i]
                    df["Stock code"] = df["Stock code"].apply(int)
                    df["Date of filing"] = pd.to_datetime(df["Serial No"].apply(lambda x: str(x)[2:10]))
                    dfs.append(df)
                except Exception as e:
                    print(e)

            DI_results = pd.concat(dfs, ignore_index=True).drop_duplicates()
            DI_results["Stock code"] = DI_results["Stock code"].astype(int)
            DI_results["Previous No. of shares"] = DI_results["Previous No. of shares"].apply(txt_transform)
            DI_results["Present No. of shares"] = DI_results["Present No. of shares"].apply(txt_transform)
            DI_results["Previous No. of shares (%)"] = DI_results["Previous No. of shares (%)"].apply(txt_transform_pct)
            DI_results["Present No. of shares (%)"] = DI_results["Present No. of shares (%)"].apply(txt_transform_pct)
            DI_results = DI_results.set_index(["Stock code", "Name of listed corporation", "Date of filing", "Date of relevant event"], drop=True).sort_values("Date of filing", ascending=False)
            forms_dict = {"IS": "1", "CS": "2", "DA": "3A", "DB": "3B", "DC": "3C", "DD": "3D"}
            DI_results["URL"] = DI_results["Serial No"].apply(lambda x: f"""https://di.hkex.com.hk/di/NSForm{forms_dict[x[0:2]]}.aspx?fn={x}""")

            return DI_results

        def sid_mapping(self):
            return  # TODO:


if __name__ == "__main__":
    #test_df = HKEX.DI(242719).all_filings()
    #print(test_df)
    print(HKEX.CCASS_participants_dict())
