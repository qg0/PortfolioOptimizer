"""Сохраняет, обновляет и загружает локальную версию данных по CPI.

    get_cpi()
"""

import time
from os import path

import numpy as np
import pandas as pd

import optimizer.getter.storage
from optimizer import download
from optimizer.settings import DATE, CPI

DATA_PATH = optimizer.getter.storage.make_data_path('macro', 'cpi.csv')

# FIXME: это не глабльная ли переменная на несколько модулей?
# MIKE: нет, периодичность может быть разная для разных данных - просто пока поставил одинаковую
UPDATE_PERIOD_IN_DAYS = 1


def save_cpi(df):
    """Сохраняет файл с данными."""
    df.to_csv(DATA_PATH, index=True, header=True)


def load_cpi():
    """Загружает данные из локальной версии и возвращает их.

    Значение sep гарантирует загрузку данных с добавленными PyCharm пробелами.
    """
    df = pd.read_csv(DATA_PATH,
                     converters={DATE: pd.to_datetime, CPI: pd.to_numeric},
                     header=0,
                     # QUESTION: не очень понятно, откуда берется такое сложное чтение
                     #           пишем csv же стандартным способом
                     # ANSWER: если смотреть файлы в PyCharm, то он вставляет пробелы для выравнивания столбцов,
                     #         после чего файлы не читаются.
                     # EP: наверняка и у PyCharm есть настройка на этот счет
                     #     выглядит как заглушка, немного странно подстраивать код под редактор
                     #     в любом случае это отдельная функция.
                     # Mike: можно отключить в PyCharm, но мне так удобнее просматривать csv-файлы
                     engine='python',
                     sep='\s*,')
    df = df.set_index(DATE)
    return df[CPI]


# FIXME: общая функция needs_update(filepath)
def cpi_need_update():
    """Обновление нужно, если прошло установленное число дней с момента обновления."""
    if time.time() - path.getmtime(DATA_PATH) > UPDATE_PERIOD_IN_DAYS * 60 * 60 * 24:
        return True
    return False


def validate(df_old, df_updated):
    """Проверяет совпадение данных для дат, присутствующих в старом фрейме."""
    if not np.allclose(df_old, df_updated[df_old.index]):
        raise ValueError('Новые данные CPI не совпадают с локальной версией.')

# FIXME: в двух функциях ниже не должно быть возврата df, это перегружает
#        лучше остановаиться на том, что фрейм всегда получает load_cpi()
# ANSWER: будет реализовано в виде класса на основе dividends
# EP: без класса пока проще отрефакторить, с классом новые сложности появятся


def update_cpi():
    df = load_cpi()
    if cpi_need_update():
        df_updated = download.cpi()
        validate(df, df_updated)
        df = df_updated
        save_cpi(df)
    return df


def create_cpi():
    """Создает с нуля файл с данными и возвращает серию с инфляцией."""
    df = download.cpi()
    save_cpi(df)
    return df


def get_cpi():
    """
    Сохраняет, обновляет и загружает локальную версию данных по CPI.

    Returns
    -------
    pd.Series
        В строках значения инфляции для каждого месяца.
    """
    if DATA_PATH.exists():
        # FIXME: здесь должны быть
        # update_cpi()
        return update_cpi()
    else:
        # create_cpi()
        return create_cpi()
    # return load_cpi()


if __name__ == '__main__':
    print(get_cpi())
