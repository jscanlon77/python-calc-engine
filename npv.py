import pyodbc
import pandas as pd
import numpy as np
import itertools
from itertools import islice
from sqlalchemy import create_engine, event
from urllib.parse import quote_plus


def createConnection():
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=LOCALHOST;'
                      'Database=InvestorAnalytics_TEST;'
                      'Trusted_Connection=yes;')
    return conn


conn = createConnection()


# This brings back all the positions in order
# now pop it in a grouped data frame so that we can calculate the npv
positions_df = pd.read_sql_query('EXEC GetPositions',conn)



#now group the data frame
grouped_positions_df = positions_df.groupby(['Equity_ID', 'Institution_ID'])

# now loop over each group and then get all the positions so that we can calculate npv
# provide a discount rate as well

discount = 0.281


equityIdList = []
institutionIdList = []
npvList = []

for name, group in grouped_positions_df:
    #Now calculate the cashflows and write out the equity, institution and date, cashflow value
    # and add to the dictionary

    npv = np.npv(discount, group['Position'])
    equityIdList.append(name[0])
    institutionIdList.append(name[1])
    npvList.append(npv)

    # now create the array to calculate xirr

df = pd.DataFrame({'Equity_ID': equityIdList, 'Institution_ID': institutionIdList, 'NPV': npvList})    
del equityIdList, institutionIdList, npvList



#Now write back to the sql in a fast way
alchemyConn =  "DRIVER={ODBC Driver 17 for SQL Server};SERVER=LOCALHOST;DATABASE=InvestorAnalytics_TEST;Trusted_Connection=yes;"
quoted = quote_plus(alchemyConn)
new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
engine = create_engine(new_con)


@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True



df.to_sql('CalculatedNPV', engine, index=False, schema="dbo", if_exists = "replace")
conn.close()