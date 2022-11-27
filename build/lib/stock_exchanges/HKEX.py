import requests
import pandas as pd
from datetime import datetime

class HKEX:

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
        df = pd.read_excel(r'https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx',skiprows=range(2),thousands=',',decimal='.', true_values="Y", false_values=' ')
        df.rename(columns = {'Spread Table\n1, 4 = Part A\n3 = Part B\n5 = Part D':'Spread Schedule'}, inplace = True)
        return df

    @classmethod
    def securities_name_dict(cls) -> dict:
        """A dictionary mapping Stock Code and Stock Name. Downloaded from https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx.

        Returns:
        ---
            dict: A dict of {int: str}. Example: {1: 'CKH HOLDINGS'}
        """
        my_dict = cls.securities_details().set_index('Stock Code')['Name of Securities'].to_dict()

        return my_dict

    @staticmethod
    def CCASS_participants_dict() -> dict:
        """A dictionary mapping the CCASS ID and the corresponding Broker Name.

        Returns:
        ---
            dict: A dict of {str: str}. Example: {'B00128': 'KAM WAH SECURITIES CO'}
        """
        participants_dict = pd.DataFrame(requests.get(f'https://www.hkexnews.hk/sdw/search/partlist.aspx?sortby=partid&shareholdingdate={datetime.today().strftime("%Y%m%d")}').json()).rename(columns={'c':'Participant ID', 'n':'Participant Name'}).set_index('Participant ID',drop=True)['Participant Name'].to_dict()
        return participants_dict


if __name__ == '__main__':
    df = HKEX.securities_name_dict()
    print(df)