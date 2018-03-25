"""Download and transform securities info to pandas DataFrames.

    Lots sizes, short names and last quotes for list of tickers:

        get_securities_info(tickers)
"""

import pandas as pd
import requests


def make_url(tickers):
    tickers = ','.join(tickers)
    return ('https://iss.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json?'
            f'securities={tickers}')

def get_raw_json(tickers):
    url = make_url(tickers)
    r = requests.get(url)
    result = r.json()
    validate_response(result, tickers)
    return result


def validate_response(data, tickers):
    n = len(tickers)
    msg = ('Количество тикеров в ответе не соответствует запросу'
           ' - возможно ошибка в написании')
    if len(data['securities']['data']) != n:
        raise ValueError(msg)
    if len(data['marketdata']['data']) != n:
        raise ValueError(msg)


def get_market_data_dataframe(raw_json):
    market_dict = raw_json['marketdata']
    market_data = pd.DataFrame(data=market_dict['data'],
                               columns=market_dict['columns'])
    # FIXME: нужно упростить и разделить
    market_data = pd.to_numeric(market_data.set_index('SECID')['LAST'])
    return market_data 


def get_securities_info_dataframe(raw_json):
    securities_dict = raw_json['securities']
    securities = pd.DataFrame(data=securities_dict['data'],
                              columns=securities_dict ['columns'])
    securities = securities.set_index('SECID')[['SHORTNAME', 'REGNUMBER', 'LOTSIZE']]
    return securities   


def make_df(raw_json):
    securities = get_securities_info_dataframe(raw_json)
    market_data = get_market_data_dataframe(raw_json)
    df = pd.concat([securities, market_data], axis=1)
    # FIXME: rename лучше чеме перезапись, с которой сложнее гарантировать порядок колонок
    df.index.name = 'TICKER'
    df.columns = ['COMPANY_NAME', 'REG_NUMBER', 'LOT_SIZE', 'LAST_PRICE']
    return df


def get_securities_info(tickers: list):
    """
    Возвращает краткое наименование, размер лота и последнюю цену.

    Parameters
    ----------
    tickers : list
        Список тикеров.

    Returns
    -------
    pandas.DataFrame
        В строках тикеры (используется написание из выдачи ISS).
        В столбцах краткое наименование, регистрационный номер, размер лота и последняя цена.
    """
    raw_json = get_raw_json(tickers)
    return make_df(raw_json)

# TODO: проанализировать дальгейшее использование, возможно не должно хранится 
#       как такой фрейм. Может быть преобразован дургому к типу хранения
# TODO(minor): много повторяющегося кода сверху, можно разбироать response

if __name__ == "__main__":
    print(get_securities_info(['UPRO', 'MRSB']))