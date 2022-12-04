import pandas as pd
from datetime import datetime, timedelta
import json
import requests


class KRX:
    @staticmethod
    def get_stock_name(stock_code: str) -> str:
        payload = "locale=en&mktsel=ALL&typeNo=0&searchText=&bld=dbms%2Fcomm%2Ffinder%2Ffinder_stkisu"
        response = requests.request("POST", "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd", data=payload, headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"})
        df = pd.DataFrame(json.loads(response.text)["block1"])
        name_dict = df.set_index("short_code")["codeName"].to_dict()
        return name_dict[str(stock_code)]

    @staticmethod
    def get_issue_id(stock_code: str) -> str:
        payload = "locale=en&mktsel=ALL&typeNo=0&searchText=&bld=dbms%2Fcomm%2Ffinder%2Ffinder_stkisu"
        response = requests.request("POST", "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd", data=payload, headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"})
        df = pd.DataFrame(json.loads(response.text)["block1"])
        issue_id_dict = df.set_index("short_code")["full_code"].to_dict()
        return issue_id_dict[str(stock_code)]

    @staticmethod
    def trading_by_investor(issue_id: str, dt: datetime = datetime.today()) -> pd.DataFrame:
        """Download Daily Trading Summary (by Investor Types). Retrieved from http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020301.

        Args:
        ---
            issue_id (str): _description_
            dt (datetime, optional): Date of the Summary. Defaults to datetime.today().

        Returns:
        ---
            pd.DataFrame:
                Index:
                    Investor Categories
                Columns:
                    Shares Bought (Net): object
                    ₩ Traded (Net): object
                    Shares Bought: object
                    ₩ Bought: object
                    Average Purchase Price (₩): object
                    Shares Sold: object
                    ₩ Sold: object
                    Average Sale Price (₩): object
        """
        # Load Data and Create DataFrame
        url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        payload = f"bld=dbms%2FMDC%2FSTAT%2Fstandard%2FMDCSTAT02301&locale=en&isuCd={issue_id}&strtDd={dt.strftime('%Y%m%d')}&endDd={dt.strftime('%Y%m%d')}&share=1&money=1"

        response = requests.request("POST", url, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"})
        df = pd.DataFrame(json.loads(response.text)["output"]).replace({",": ""}, regex=True).iloc[:, :-1]

        # Process and Calculate Data
        df = df.apply(pd.to_numeric, errors="ignore")
        df["ASK_TRDVAL"] = -df["ASK_TRDVAL"]
        df["ASK_TRDVOL"] = -df["ASK_TRDVOL"]
        df["AVG_SELL_PX"] = df["ASK_TRDVAL"] / df["ASK_TRDVOL"]
        df["AVG_BUY_PX"] = df["BID_TRDVAL"] / df["BID_TRDVOL"]

        # Format Numbers
        df = df.replace(0, pd.NaT).fillna("")
        int_col = ["ASK_TRDVOL", "BID_TRDVOL", "NETBID_TRDVOL"]
        KRW_col = ["ASK_TRDVAL", "BID_TRDVAL", "NETBID_TRDVAL", "AVG_SELL_PX", "AVG_BUY_PX"]
        df[int_col] = df[int_col].applymap(lambda x: "" if x == "" else f"{x:,.0f}")
        df[KRW_col] = df[KRW_col].applymap(lambda x: "" if x == "" else f"₩{x:,.0f}")

        # Format columns
        df.columns = ["Investor Type", "Shares Sold", "Shares Bought", "Shares Bought (Net)", "₩ Sold", "₩ Bought", "₩ Traded (Net)", "Average Sale Price (₩)", "Average Purchase Price (₩)"]  # Rename
        df = df[["Investor Type", "Shares Bought (Net)", "₩ Traded (Net)", "Shares Bought", "₩ Bought", "Average Purchase Price (₩)", "Shares Sold", "₩ Sold", "Average Sale Price (₩)"]]  # Reorder

        # Format Index
        df = df.set_index("Investor Type")
        df.index = [
            "Financial Investment (Local IB Prop Desk)",
            "Insurance",
            "Investment Trust (Hedge Funds)",
            "Private Equity Funds",
            "Bank",
            "Other Finances",
            "Government Pension Funds",
            "===Subtotal of Local Institutions===",
            "Other Corporations",
            "Individuals",
            "Foreigners (Institutional)",
            "Other Foreigners (Individuals)",
            "=========Total=========",
        ]
        return df

    @staticmethod
    def foreign_ownership(issue_id: str, start_date: datetime = datetime.today() - timedelta(days=30), end_date: datetime = datetime.today()) -> pd.DataFrame:
        """Download Last 30 Days Foreign Ownership. Retrieved from http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020301.

        Args:
        ---
            issue_id (str): _description_
            dt (datetime, optional): Date of the Summary. Defaults to datetime.today().

        Returns:
        ---
            pd.DataFrame:
                Index:
                    Investor Categories
                Columns:
                    Close: int64
                    U/D: int64
                    Change: int64
                    %Change: float64
                    No. of listed shares: int64
                    No. of shares of foreign ownership: int64
                    Foreign ownership ratio (%): float64
                    Foreign ownership limit quantity: int64
                    Exhaustion rate (%): float64
        """
        # Foreign Ownership
        url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
        cols = ["Date", "Close", "U/D", "Change", "%Change", "No. of listed shares", "No. of shares of foreign ownership", "Foreign ownership ratio", "Foreign ownership limit quantity", "Exhaustion rate"]
        payload = f"bld=dbms%2FMDC%2FSTAT%2Fstandard%2FMDCSTAT03702&locale=en&isuCd={issue_id}&strtDd={start_date.strftime('%Y%m%d')}&endDd={end_date.strftime('%Y%m%d')}"
        response = requests.request("POST", url, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"})
        df = pd.DataFrame(json.loads(response.text)["output"]).replace({",": ""}, regex=True)
        df.columns = cols
        df = df.set_index("Date")
        df.index = pd.DatetimeIndex(df.index)
        df = df.apply(pd.to_numeric, errors="ignore")
        df = df.iloc[::-1]
        df.columns = ["Close", "U/D", "Change", "%Change", "No. of listed shares", "No. of shares of foreign ownership", "Foreign ownership ratio (%)", "Foreign ownership limit quantity", "Exhaustion rate (%)"]
        return df

    @staticmethod
    def securities_details() -> pd.DataFrame:
        # Load Data and Create DataFrame
        url = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

        payload = r"bld=dbms%2FMDC%2FSTAT%2Fstandard%2FMDCSTAT01901&locale=en&mktId=ALL"
        response = requests.post(url, data=payload, headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"})
        df = pd.DataFrame(json.loads(response.text)["OutBlock_1"])

        # Specific dtypes
        # df['ISU_SRT_CD'] = df['ISU_SRT_CD'].astype(int)
        df["LIST_DD"] = pd.to_datetime(df["LIST_DD"])
        df["PARVAL"] = df["PARVAL"].apply(lambda x: x.replace(",", "").replace("No par value", "0")).astype(float)
        df["LIST_SHRS"] = df["LIST_SHRS"].apply(lambda x: x.replace(",", "")).astype("int64")

        df.columns = ["ISIN", "Stock Code", "Stock Name (Korean)", "Stock Abbreviation (Korean)", "Stock Name (English)", "Listing Date", "Market Type", "Security Type", "Company Category", "Share Type", "Par Value", "Shares Issued"]
        return df


if __name__ == "__main__":
    print(KRX.securities_details())
