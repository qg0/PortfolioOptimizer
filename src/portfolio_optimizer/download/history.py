"""Download and transform daily quotes to pandas DataFrames.

   1. Single ticker daily price and volumes:

        get_quotes_history(ticker, start_date)

   2. MOEX Russia Net Total Return (Resident) Index:

        get_index_history(start_date)
"""

import datetime

import pandas as pd
import requests

from portfolio_optimizer.settings import DATE, CLOSE_PRICE, VOLUME


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


class Quotes:
    """
    Представление ответа сервера по отдельному тикеру.
    """
    base = 'https://iss.moex.com/iss/history/engines/stock/markets/shares/securities'

    def __init__(self, ticker, start_date):
        self.ticker, self.start_date = ticker, start_date
        self.block_position = 0
        self.data = None
        self.load()

    @property
    def url(self):
        """Формирует url для запроса данных с MOEX ISS."""
        return make_url(self.base, self.ticker,
                        self.start_date, self.block_position)

    def load(self):
        """Загружает и проверяет json с данными."""
        self.data = get_json(self.url)
        self._validate()

    def _validate(self):
        if self.block_position == 0 and len(self) == 0:
            raise ValueError(f'Пустой ответ. Проверьте запрос: {self.url}')

    def __len__(self):
        return len(self.values)

    def __bool__(self):
        return self.__len__() > 0

    #  В ответе сервера есть словарь:
    #   - по ключу history - словарь с историей котировок
    #   - во вложеном словаре есть ключи columns и data с масивами описания
    #     колонок и данными.

    @property
    def values(self):
        """Извлекает данные и json."""
        return self.data['history']['data']

    @property
    def columns(self):
        """"Возвращает наименование колонок данных."""
        return self.data['history']['columns']

    @property
    def df(self):
        """Raw dataframe from *self.data['history']*"""
        return pd.DataFrame(data=self.values, columns=self.columns)

    # WONTFIX: для итератора необходимы два метода: __iter__() и __next__()
    #          возможно, переход к следующему элементу может быть по-другому
    #          распределен между этими методами
    def __iter__(self):
        return self

    def __next__(self):
        # если блок непустой
        if self:
            # используем текущий результат парсинга
            current_dataframe = self.dataframe
            # перещелкиваем сдаиг на следующий блок и получаем новые данные
            self.block_position += len(self)
            self.load()
            # выводим текущий результат парсинга
            return current_dataframe
        else:
            raise StopIteration

    @property
    def dataframe(self):
        """Выбирает из сырого DataFrame только с необходимые колонки - даты, цены закрытия и объемы."""
        df = self.df
        df[DATE] = pd.to_datetime(df['TRADEDATE'])
        df[CLOSE_PRICE] = pd.to_numeric(df['CLOSE'])
        df[VOLUME] = pd.to_numeric(df['VOLUME'])
        return df[[DATE, CLOSE_PRICE, VOLUME]]


class Index(Quotes):
    """
    Представление ответа сервера - данные по индексу полной доходности MOEX.

    """
    base = 'http://iss.moex.com/iss/history/engines/stock/markets/index/boards/RTSI/securities'
    ticker = 'MCFTRR'

    def __init__(self, start_date):
        super().__init__(self.ticker, start_date)

    @property
    def dataframe(self):
        """Выбирает из сырого DataFrame только с необходимые колонки - даты и цены закрытия."""
        df = self.df
        df[DATE] = pd.to_datetime(df['TRADEDATE'])
        df[CLOSE_PRICE] = pd.to_numeric(df['CLOSE'])
        return df[[DATE, CLOSE_PRICE]].set_index(DATE)


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
    return pd.concat(Index(start_date))[CLOSE_PRICE]


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
    gen = Quotes(ticker, start_date)
    df = pd.concat(gen, ignore_index=True)
    # Для каждой даты выбирается режим торгов с максимальным оборотом
    df = df.loc[df.groupby(DATE)[VOLUME].idxmax()]
    df = df.set_index(DATE).sort_index()
    return df


if __name__ == '__main__':
    z = get_index_history(start_date=datetime.date(2017, 10, 2))
    print(z.head())
    print(z.tail())
