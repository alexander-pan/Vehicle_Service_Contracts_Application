import numpy as np
import pandas as pd
import pyodbc
from datetime import datetime as dt, timedelta
from dateutil.relativedelta import *
from collections import OrderedDict
import sys
sys.path.append('../../../passwords')
from sunpath_dbcreds import server,database,username,password

cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

q1 = "select * from SPAdmin.dbo.SPA_FundingBankStat order by cleardate asc;"
q2 = "select * from SPAdmin.dbo.SPA_SPDepositsStat order by cleardate asc;"
q3 = "select * from SPAdmin.dbo.SPA_SPPlugStat order by cleardate asc;"
q4 = "select * from SPAdmin.dbo.SPA_SPPaymentsStat where groupname='Packs' order by cleardate asc;"
q5 = "select * from SPAdmin.dbo.SPA_SPPlugStat where type='Pack' order by cleardate asc;"

df1 = pd.read_sql(q1,cnxn)
df2 = pd.read_sql(q2,cnxn)
df3 = pd.read_sql(q3,cnxn)
df4 = pd.read_sql(q4,cnxn)
df5 = pd.read_sql(q5,cnxn)

df1.to_pickle('../data/SPA_SPBankingStats.pkl')
