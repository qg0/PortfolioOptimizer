"""Local file storage for pandas DataFrames."""

import time
from pathlib import Path

import pandas as pd
import numpy as np

from optimizer import settings


def make_data_path(subfolder: str, filename: str) -> str:
    """Создает подкаталог *subfolder* в директории данных и 
       возвращает путь к файлу *file_name* в нем."""
    folder = settings.DATA_PATH / subfolder
    if not folder.exists():
        folder.mkdir(parents=True)
    return str(folder / filename)

class LocalFrame:
    """Wrapper class to save and read dataframe with date index and 
      numeric values by column."""
      
    def __init__(self, subfolder: str, filename: str):
        self.path = make_data_path(subfolder, filename)        
      
    def exists(self) -> bool:
        """Проверка существования файла."""
        return Path(self.path).exists()

    def updated_days_ago(self) -> float:
        """Количество дней с последнего обновления файла.

        https://docs.python.org/3/library/os.html#os.stat_result.st_mtime
        """
        time_updated = Path(self.path).stat().st_mtime
        lag_sec = time.time() - time_updated
        return lag_sec / (60 * 60 * 24)

    def save(self, df):
        """Сохраняет DataFrame или Series с заголовками."""
        df.to_csv(self.path, index=True, header=True)

    def read_dataframe(self):
        """Загружает данные из файла.

        Значение sep обеспечивает корректную загрузку с лидирующими пробелами, 
        вставленными PyCharm.
        """
        return pd.read_csv(self.path,
                         converters={0:pd.to_datetime},
                         header=0,
                         index_col=0,                         
                         engine='python',
                         sep='\s*,')

class LocalFile:
    """Обеспечивает функционал сохранения, проверки наличия, загрузки и даты изменения для файла.

     Реализована поддержка для DataFrames и Series с корректным сохранением заголовков.
     """

    def __init__(self, subfolder: str, filename: str, converters: dict):
        """
        Инициирует объект.

        Parameters
        ----------
        subfolder
            Подкаталог, где хранятся данные.
        filename
            Наименование файла с данными.
        converters
            Упорядочений словарь с конвертерами данных. Первый элементы индекс, последующие столбцы данных.
            Если колонка одна, то функции загрузки будут возвращать Series.
        """
        self.path = make_data_path(subfolder, filename)
        self.converters = converters
        # EP: здесь вопрос к структуре данных - почему все сложно?
        columns = list(converters.keys())
        self._index = columns[0]
        self._data_columns = columns[1:]
        # FIXME: почему мы не можем выдавать всегда фрейм?
        #        если строго нужен Series _ это другой класс
        # Если колонок с данными 1, то надо выдавать Series при загрузке
        if len(self._data_columns) == 1:
            self._data_columns = self._data_columns[0]

    def exists(self):
        """Проверка существования файла."""
        return self.path.exists()

    def updated_days_ago(self):
        """Количество дней с последнего обновления файла.

        https://docs.python.org/3/library/os.html#os.stat_result.st_mtime
        """
        time_updated = Path(self.path).stat().st_mtime
        lag_sec = time.time() - time_updated
        return lag_sec / (60 * 60 * 24)

    def save(self, df):
        """Сохраняет DataFrame или Series с заголовками."""
        df.to_csv(self.path, index=True, header=True)

    def read(self):
        """Загружает данные из файла.

        Значение sep обеспечивает корректную загрузку с лидирующими пробелами, вставленными PyCharm.
        """
        df = pd.read_csv(self.path,
                         converters=self.converters,
                         header=0,
                         engine='python',
                         sep='\s*,')
        df = df.set_index(self._index)
        # FIXME: почему мы ограничиваем на чтении колонки?
        #        приницип такой - что записываем, то и читаем, здесь не должно
        #        долнительнйо логики        
        return df[self._data_columns]


class DataProvider:
    def __init__(self, donwloader, subfolder: str, filename: str):
        self.local_storage = LocalFrame(subfolder, filename)
        self.download_method = donwloader
        if not self.local_storage.exists():
            self.update(must_validate=False)
            
    def get_dataframe(self):
        return self.local_storage.read_dataframe()        
            
    def is_new_data_consistent(self, new_df):
        existing_df = self.get()
        ix = existing_df.index
        return np.allclose(existing_df, new_df[ix])
    
    def update(self, must_validate=True):
        df = self.download_method()
        if must_validate and not self.is_new_data_consistent(df):
            raise ValueError('Загруженные данные не совпадают с локальной версией.')
        self.local_storage.save(df) 

# NOTE: Use generic local file wrapper 
# DATA_PATH = Path(__file__).parents[2] / 'data'
# FILE_CPI = LocalFile('CPI.csv')
