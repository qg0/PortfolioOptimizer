"""Local file storage for pandas DataFrames."""

import time
from pathlib import Path

import pandas as pd

from optimizer import settings


def make_data_path(subfolder: str, filename: str) -> str:
    """Создает подкаталог *subfolder* в директории данных и 
       возвращает путь к файлу *file_name* в нем."""
    folder = settings.DATA_PATH / subfolder
    if not folder.exists():
        folder.mkdir(parents=True)
    return str(folder / filename)

class LocalFrame:
    """Wrapper class to save locally and read dataframe with date index and 
      numeric values by column.
    """      
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
        return float(lag_sec / (60 * 60 * 24))

    def save_dataframe(self, df):
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
        

class DataProvider:
    """Класс для работы c локальной версией фрейма и для полученя данных из интернета. 
       
       Класс предоставляет методы для: 
       - инициализации локальных данных 
       - получения данных из интернета
       - перезаписи локальной копии
       - чтения лоакальной копии       
       
       Класс ничего не знает о требуемой периодичности обновления и способах 
       верификации данных, это не его забота.  
       
    """    
    def __init__(self, download_method, 
                       download_all_method,
                       ticker: str, 
                       subfolder: str):
        """
        Args:
            download_method - функция загрузки данных по тикеру и другим аргументам
                              чаще всего это обычная функция, которая используется 
                              для скачивания данных, принимает те же аргументы
            download_all_method - функция для загрузки всех имеющихся данных по 
                                  тикеру и не принимает аргументы
            ticker: str -  тикер, напрмиер 'GAZP'
            subfolder: str - папка для хранения, например 'dividends'
            
        Идея с двумя методами состоит в том, чтобы перенести отвественность
        за подготовку функций до момента вызова конструтора класса, 
        а сам класс делал меньше предположений об их работе.
        """
        
        filename = f'{ticker.upper()}.csv'
        self.local_storage = LocalFrame(subfolder, filename)
        self.download_method = download_method
        self.download_all_method = download_all_method
        self.ticker = ticker
        if not self.exists():
            print(f'Creating local data for {self.ticker}...')            
            self.create()
            print('Done')
            
    def exists(self):
        return self.local_storage.exists()        

    def get_web_dataframe(self, *arg, **kwarg):
        return self.download_method(self.ticker, *arg, **kwarg) 
            
    def get_local_dataframe(self):
        return self.local_storage.read_dataframe()        
            
    def create(self):
        df = self.download_all_method()
        self.overwrite(df) 

    def overwrite(self, df: pd.DataFrame):
        self.local_storage.save_dataframe(df) 
