"""Downloader and parser for CPI."""

from datetime import date

import pandas as pd

from optimizer.storage import DataProvider

URL_CPI = 'http://www.gks.ru/free_doc/new_site/prices/potr/I_ipc.xlsx'
PARSING_PARAMETERS = dict(sheet_name='ИПЦ', header=3, skiprows=[4], skip_footer=3)


def validate(df):
    months, _ = df.shape
    first_year = df.columns[0]
    first_month = df.index[0]
    if months != 12:
        raise ValueError('Data must contain 12 rows only')
    if first_year != 1991:
        raise ValueError('First year must be 1991')
    if first_month != 'январь':
        raise ValueError('First month must be January')


def parse_xls(url=URL_CPI, excel_reading_parameters=PARSING_PARAMETERS):
    """
    Загружает данные по месячному CPI с сайта ФСГС и возвращает фрейм 
    с этими данными.

    Returns
    -------
    pd.DataFrame
        В строках значения инфляции для каждого месяца.
    """
    df = pd.read_excel(url, **excel_reading_parameters)
    validate(df)
    size = df.shape[0] * df.shape[1]
    first_year = df.columns[0]
    start_dt = date(first_year, 1, 31)
    # create new DataFrame
    index = pd.DatetimeIndex(name='DATE', freq='M', start=start_dt, periods=size)
    flat_data = df.values.reshape(size, order='F')
    df = pd.DataFrame(flat_data, index=index, columns=['CPI'])
    return df.dropna().div(100)


def cpi_provider():
    return DataProvider(parse_xls, parse_xls, 'CPI', 'macro')


def get_cpi():
    provider = cpi_provider()
    return provider.get_local_dataframe()


def update_cpi():
    provider = cpi_provider()
    provider.create()

if __name__ == '__main__':
    print(get_cpi().tail())
