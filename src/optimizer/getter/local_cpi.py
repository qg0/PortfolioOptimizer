"""Сохраняет, обновляет и загружает локальную версию данных по CPI.

    get_cpi()
"""


import pandas as pd

from optimizer.download.cpi import get_monthly_cpi
from optimizer.storage import LocalFrame, DataProvider

CPI_FILE = LocalFrame('macro', 'cpi.csv')
CPI_SOURCE = DataProvider(CPI_FILE,  get_monthly_cpi)


from io import StringIO

z = pd.DataFrame(get_monthly_cpi())
txt = z.to_csv()
f = StringIO(txt)
df = pd.read_csv(f, converters={0:pd.to_datetime}, index_col=0)
print(df.head())

CPI_FILE.save(z)
df2 = CPI_FILE.read_dataframe()


print(CPI_SOURCE.get().head()) 
        
        

