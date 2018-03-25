"""Download and transform daily quotes to pandas DataFrames.

   1. Single ticker daily price and volumes:

        get_quotes_history(ticker, start_date)

   2. MOEX Russia Net Total Return (Resident) Index:

        get_index_history(start_date)
"""

import datetime

import pandas as pd
import requests

from optimizer.storage import DataProvider


def get_json(url: str):
    """Return json found at *url*."""
    response = requests.get(url)
    return response.json()


def make_url(base: str, ticker: str, start_date=None, block_position=0):
    """Create url based on components.

    Parameters
    ----------
    base
        Основная часть url.
    ticker
        Наименование тикера.
    start_date : date.time or None
        Начальная дата котировок. Если предоставлено значение None
        - данные запрашиваются с начала имеющейся на сервере ISS
        истории котировок.
    block_position : int
        Позиция курсора, начиная с которой необходимо получить очередной блок
        данных. При большом запросе сервер ISS возвращает данные блоками обычно
        по 100 значений. Нумерация позиций в ответе идет с 0.

    Returns
    -------
    str
        Строка url для запроса.
    """
    if not base.endswith('/'):
        base += '/'
    url = base + f'{ticker}.json'
    query_args = [f'start={block_position}']
    if start_date:
        if not isinstance(start_date, datetime.date):
            raise TypeError(start_date)
        query_args.append(f"from={start_date:%Y-%m-%d}")
    arg_str = '&'.join(query_args)
    return f'{url}?{arg_str}'

class Quote:
    """
    Представление ответа сервера по отдельному тикеру.
    """
    base = 'https://iss.moex.com/iss/history/engines/stock/markets/shares/securities'

    def __init__(self, ticker, start_date):
        self.ticker, self.start_date = ticker, start_date
        self.block_position = 0
        self.load()
        
    def load(self):    
        self.data = get_json(self.url)
        if self.block_position == 0 and len(self) == 0:
            raise ValueError(f'Пустой ответ по запросу {self.url}')

    @property
    def url(self):
        """Формирует url для запроса данных с MOEX ISS."""
        return make_url(self.base, 
                        self.ticker,
                        self.start_date, 
                        self.block_position)
    @property
    def values(self):
        """Извлекает данные и json."""
        return self.data['history']['data']

    @property
    def columns(self):
        """"Возвращает наименование колонок данных."""
        return self.data['history']['columns']

    @property
    def raw_df(self):
        """Raw dataframe from *self.data['history']*"""
        return pd.DataFrame(data=self.values, columns=self.columns)

    @property
    def renamed_df(self):
        mapper = {'TRADEDATE': 'DATE', 'CLOSE': 'CLOSE_PRICE'}
        df = self.raw_df.rename(index=str, columns=mapper)
        return df[['DATE', 'CLOSE_PRICE', 'VOLUME']]
    
    @property
    def dataframe(self):
        """Выбирает из сырого DataFrame только необходимые колонки 
           - даты и цены закрытия."""
        df = self.renamed_df
        df['DATE'] = pd.to_datetime(df['DATE'])
        return df.set_index('DATE')

    def __len__(self):
        return len(self.values)

    def __bool__(self):
        return self.__len__() > 0
    
    def iterate(self):
        while self:
            yield self.renamed_df
            self.block_position += len(self)
            self.load() 


class Index(Quote):
    """
    Представление ответа сервера - данные по индексу полной доходности MOEX.

    """
    base = 'http://iss.moex.com/iss/history/engines/stock/markets/index/boards/RTSI/securities'
    ticker = 'MCFTRR'

    def __init__(self, start_date):
        super().__init__(self.ticker, start_date)

    @property
    def renamed_df(self):
        mapper = {'TRADEDATE': 'DATE', 'CLOSE': 'CLOSE_PRICE'}
        df = self.raw_df.rename(index=str, columns=mapper)
        return df.set_index('DATE')[['CLOSE_PRICE']]

    
def get_index_history(start_date=None):
    """
    Возвращает котировки индекса полной доходности с учетом российских налогов
    начиная с даты *start_date*.

    Parameters
    ----------
    start_date : datetime.date or None
        Начальная дата котировок.

    Returns
    -------
    pandas.Series
        В строках даты торгов.
        В столбцах цена закрытия индекса полной доходности.
    """
    gen = Index(start_date).iterate()
    return pd.concat(gen)


def get_quotes_history(ticker, start_date=None):
    """
    Возвращает историю котировок тикера начиная с даты *start_date*.

    Parameters
    ----------
    ticker : str
        Тикер, например, 'MOEX'.

    start_date : datetime.date or None
        Начальная дата котировок.

    Returns
    -------
    pandas.DataFrame
        В строках даты торгов.
        В столбцах [CLOSE, VOLUME] цена закрытия и оборот в штуках.
    """
    gen = Quote(ticker, start_date).iterate()
    df = pd.concat(gen, ignore_index=True)
    # Для каждой даты выбирается режим торгов с максимальным оборотом
    ix = df.groupby('DATE')['VOLUME'].idxmax()
    df = df.iloc[ix]
    return df.set_index('DATE').sort_index()
        
        
def make_quote_provider(ticker):
    def _download_all():
        return get_quotes_history(ticker, None)
    return DataProvider(get_quotes_history, _download_all, ticker, 'quotes')


def make_index_provider():
    def _download_all():
        return get_index_history(None)    
    return DataProvider(get_index_history, _download_all, 'MCFTRR', 'index')


def get_quote(ticker):
    if ticker == 'MCFTRR' or ticker.lower() == 'index':
        provider = make_index_provider()
    else:
        provider = make_quote_provider(ticker)
    return provider.get_local_dataframe()


if __name__ == '__main__':
    dt = datetime.date(2017, 10, 2)
    i = Quote('GAZP', dt)
    df = i.dataframe.head()
    print(df)
    
    df_sber = get_quote('GAZP')
    print(df_sber.head())