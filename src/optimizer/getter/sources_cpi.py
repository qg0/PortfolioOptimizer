"""Сохраняет, обновляет и загружает локальную версию данных по CPI.

    get_cpi()
    CPI_SOURCE.update()
    CPI_SOURCE.get_dataframe()
    
"""

from optimizer.download.cpi import get_monthly_cpi
from optimizer.storage import DataProvider

# мы перенаправляем функцию скачивания в файл 
CPI_SOURCE = DataProvider(get_monthly_cpi, 'macro', 'cpi.csv')

def get_cpi():
    return CPI_SOURCE.get_dataframe()

if __name__ == '__main__':
    cpi_df = CPI_SOURCE.get_dataframe()
    
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

