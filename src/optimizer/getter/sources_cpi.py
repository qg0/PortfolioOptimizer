"""Сохраняет, обновляет и загружает локальную версию данных по CPI.

    CPI_SOURCE.update()
    CPI_SOURCE.get_dataframe()
    
"""

from optimizer.download.cpi import get_monthly_cpi
from optimizer.storage import DataProvider

CPI_SOURCE = DataProvider(get_monthly_cpi, 'macro', 'cpi.csv')

# FIMXE: use in testing
#from io import StringIO
#
#z = pd.DataFrame(get_monthly_cpi())
#txt = z.to_csv()
#f = StringIO(txt)
#df = pd.read_csv(f, converters={0:pd.to_datetime}, index_col=0)
#print(df.head())
#
#CPI_FILE.save(z)
#df2 = CPI_FILE.read_dataframe()
#
#
#print(CPI_SOURCE.get().head()) 

