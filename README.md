# global-exchanges

A scraper to retrieve publicly available non-price information from stock exchanges / index provider.

## Supported Exchanges & Methods

### Hong Kong Stock Exchange (HKEX)

1. [Short-selling Eligible Stocks](https://www.hkex.com.hk/Services/Trading/Securities/Securities-Lists/Designated-Securities-Eligible-for-Short-Selling?sc_lang=en)
2. [Aggregated Reportable Short Positions](https://www.sfc.hk/en/Regulatory-functions/Market/Short-position-reporting/Aggregated-reportable-short-positions-of-specified-shares)
3. [List of All Securities](https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx)
4. Southbound Eligible Stocks ([SZSE](http://www.szse.cn/szhk/hkbussiness/underlylist/), [SSE](http://www.sse.com.cn/services/hkexsc/disclo/eligible/))
5. [Shares Repurchases](https://www3.hkexnews.hk/reports/sharerepur/sbn.asp)
6. [Company News](https://www.hkexnews.hk/index.htm)
7. [Disclosure of Interests](https://www2.hkexnews.hk/Shareholding-Disclosures/Disclosure-of-Interests?sc_lang=en)


### CCASS (Shares Custodian System under HKEX)

[Central Clearing and Settlement System (CCASS)](https://www3.hkexnews.hk/sdw/search/searchsdw.aspx) allows investors to track share holdings of brokers (on T+2 basis).
Could be a useful tool to be aware of share activities from major shareholders when combined with Disclosure of Interestss / ownership analysis.

Here I am only providing a framework to scraping the data into SQL tables.

### Hang Seng Index (HSI)

1. Constituents of indexes
2. Constituent Weight (as per latest factsheet)

### Korean Stock Exchange (KRX)

1. [Trading Statistics, by investor](http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020301)
2. [Foreign Ownership](https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020503)

## Exchanges to add

* [ ] New York Stock Exchange (NYSE)
* [ ] S&P Global (SPX)
