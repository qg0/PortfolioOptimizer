"""Load and update local data for dividends history and returns pandas DataFrames.

    get_dividends(tickers)
"""

import pandas as pd

from optimizer import download
from optimizer.storage import DataProvider

DIVIDENDS_FOLDER = 'dividends'

def make_dividend_download_method(ticker):
    def downloader():
        return download.dividends(ticker)
    return downloader


def make_dividend_data_provider(ticker: str):
    downloader = make_dividend_download_method(ticker)    
    filename = f'{ticker.upper()}.csv'
    return DataProvider(downloader, DIVIDENDS_FOLDER, filename)
        

def get_dividend(ticker): 
    df = make_dividend_data_provider(ticker).get_dataframe().drop_duplicates() 
    df.columns = [ticker]
    return df

def get_dividends(tickers: list):
    """
    Сохраняет, при необходимости обновляет и возвращает дивиденды для тикеров.

    Parameters
    ----------
    tickers
        Список тикеров.

    Returns
    -------
    pd.DataFrame
        В строках - даты выплаты дивидендов.
        В столбцах - тикеры.
        Значения - выплаченные дивиденды.
    """
    ticker_dataframes = [get_dividend(ticker) for ticker in tickers]
    return pd.concat(ticker_dataframes, axis=1).fillna(0)


if __name__ == '__main__':
    # ERROR - в исходных данных дублирование индекса 
    b = make_dividend_data_provider('SBERP').get_dataframe()
    assert b.index.is_unique is False
    
    tickers = ['GAZP', 'SBERP']
    print(get_dividends(tickers))
