import numpy as np
import pandas as pd
import pyodbc
from datetime import datetime as dt, timedelta
from dateutil.relativedelta import *
from collections import OrderedDict
import sys
import os

home = os.environ['HOME']
sys.path.append(home)
from sunpath_creds.dbcreds import server,database,username,password

cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

DESCR_KEYS = {
    'Administrator Funding' : 'Deposits',
    'Insurance Reserve Funding' : 'Insurance',
    'Paylink Chargeback Fee' : 'Customer Collections',
    'Paylink Customer Collection' : 'Customer Collections',
    'Paylink Customer Collection Reversal' : 'Customer Collections',
    'Paylink Processing Fee' : 'Customer Collections',
    'Paylink Processing Fee Reversal' : 'Customer Collections',
    'Paylink Returned Payment Fee' : 'Customer Collections',
    'RC (Debit Seller Wire)' : 'Deposits',
    'Returned Premium - Admin' : 'Deposits',
    'Returned Premium - Ins Reserve' : 'Deposits',
    'Reverse Administrator Funding' : 'Payments',
    'Reverse Insurance Reserve Funding' : 'Collections',
    'Reverse Returned Premium - Admin' : 'Collections',
    'Reverse Returned Premium - Ins Reserve' : 'Collections',
    'Reverse Seller Funding' : 'Collections',
    'Seller Funding' : 'Payments'
}

#Used in App3
q1 = "select * from SPAdmin.dbo.SPA_SPBankingStat order by cleardate asc;"
q2 = "select * from SPAdmin.dbo.SPA_SPDepositsStat order by cleardate asc;"
q3 = "select * from SPAdmin.dbo.SPA_SPPlugStat order by cleardate asc;"
q4 = "select * from SPAdmin.dbo.SPA_SPPaymentsStat where groupname='Packs' order by cleardate asc;"
q5 = "select * from SPAdmin.dbo.SPA_Funded_Contracts order by cleardate asc;"

#Used in App4
q6 = """select ClearDate, Description, Notes, Amount as BnkAmt
        from SPAdmin.dbo.SPA_SPBankingStat order by ClearDate asc;"""
q7 = """select ClearDate, Description, Notes, Account, Amount as PlugAmt
        from SPAdmin.dbo.SPA_SPPlugStat order by ClearDate asc;"""
q8 = """select ClearDate,Vendor,GroupName, sum(PaymentAmount) as PaymntsAmt
        from SPAdmin.dbo.SPA_SPPaymentsStat
        group by Vendor,GroupName,ClearDate order by ClearDate,Vendor;"""
q9 = """select ClearDate, sum(insurancereserve) as InsRsvAmt
        from SPAdmin.dbo.SPA_Funded_Contracts group by cleardate;"""
q10 = """select ClearDate, PaidTo as Payee, DisburseType as GroupName, PaymentAmount as DepAmt
        from SPAdmin.dbo.SPA_SPDepositsStat order by ClearDate asc;"""

#App 5,6
q11 = "select * from SPAdmin.dbo.SPA_FundingBankStat order by cleardate asc;"
q12 = """
SELECT TxDate,TxAmount,FTL.PosOrNegTx, TxDescription,FTL.PaidTo,FTL.PaidFrom
FROM dbo.SPF_Funding_Transaction_Log AS FTL
JOIN dbo.SPF_Funding_Transaction_Codes AS FTC
ON FTL.TxCode = FTC.TxCode
WHERE (FTL.CashTx = 1) AND (FTL.PolicyNumber IS NOT NULL);
"""

df1 = pd.read_sql(q1,cnxn)
df2 = pd.read_sql(q2,cnxn)
df3 = pd.read_sql(q3,cnxn)
df4 = pd.read_sql(q4,cnxn)
df5 = pd.read_sql(q5,cnxn)

df6 = pd.read_sql(q6,cnxn)
df6['ClearDate'] = pd.to_datetime(df6['ClearDate'],format="%m/%d/%y")
df6['ClearDate'] = df6['ClearDate'].apply(lambda x: x.date())

df7 = pd.read_sql(q7,cnxn)
df7['ClearDate'] = pd.to_datetime(df7['ClearDate'],format="%m/%d/%y")
df7['ClearDate'] = df7['ClearDate'].apply(lambda x: x.date())

df8 = pd.read_sql(q8,cnxn)
df9 = pd.read_sql(q9,cnxn)
df10 = pd.read_sql(q10,cnxn)

df11 = pd.read_sql(q11,cnxn)
keys = pd.read_csv('./SunPath Type to Account Mapping.csv',index_col=0)
keys = keys.to_dict()['Column for recon']
df11['Category'] = df11['Type'].apply(lambda x: keys[x])

df12 = pd.read_sql(q12,cnxn)
df12['Amount'] = df12['TxAmount']*df12['PosOrNegTx']
df12['Category'] = df12['TxDescription'].apply(lambda x: DESCR_KEYS[x])

#App3 Data Tables
"""path1 = '{0}/Sunpath/static/data/SPA_SPBankingStat.pkl'.format(home)
path2 = '{0}/Sunpath/static/data/SPA_SPDepositsStat.pkl'.format(home)
path3 = '{0}/Sunpath/static/data/SPA_SPPlugStat.pkl'.format(home)
path4 = '{0}/Sunpath/static/data/SPA_SPPaymentsStat.pkl'.format(home)
path5 = '{0}/Sunpath/static/data/SPA_Funded_Contracts.pkl'.format(home)"""

path1 = '{0}/Desktop/Sunpath/static/data/SPA_SPBankingStat.pkl'.format(home)
path2 = '{0}/Desktop/Sunpath/static/data/SPA_SPDepositsStat.pkl'.format(home)
path3 = '{0}/Desktop/Sunpath/static/data/SPA_SPPlugStat.pkl'.format(home)
path4 = '{0}/Desktop/Sunpath/static/data/SPA_SPPaymentsStat.pkl'.format(home)
path5 = '{0}/Desktop/Sunpath/static/data/SPA_Funded_Contracts.pkl'.format(home)

df1.to_pickle(path1)
df2.to_pickle(path2)
df3.to_pickle(path3)
df4.to_pickle(path4)
df5.to_pickle(path5)

#App4 Data Tables
"""path6 = '{0}/Sunpath/static/data/Banking_Transaction.pkl'.format(home)
path7 = '{0}/Sunpath/static/data/Plug_Other.pkl'.format(home)
path8 = '{0}/Sunpath/static/data/Payments.pkl'.format(home)
path9 = '{0}/Sunpath/static/data/SPF_Premium.pkl'.format(home)
path10 = '{0}/Sunpath/static/data/Deposits.pkl'.format(home)"""

path6 = '{0}/Desktop/Sunpath/static/data/Banking_Transaction.pkl'.format(home)
path7 = '{0}/Desktop/Sunpath/static/data/Plug_Other.pkl'.format(home)
path8 = '{0}/Desktop/Sunpath/static/data/Payments.pkl'.format(home)
path9 = '{0}/Desktop/Sunpath/static/data/SPF_Premium.pkl'.format(home)
path10 = '{0}/Desktop/Sunpath/static/data/Deposits.pkl'.format(home)

df6.to_pickle(path6)
df7.to_pickle(path7)
df8.to_pickle(path8)
df9.to_pickle(path9)
df10.to_pickle(path10)

#App5,6 DataTables
"""path11 = '{0}/Sunpath/static/data/SPA_FundingBankStat.pkl'.format(home)
path12 = '{0}/Sunpath/static/data/TransactionLog.pkl'.format(home)"""

path11 = '{0}/Desktop/Sunpath/static/data/SPA_FundingBankStat.pkl'.format(home)
path12 = '{0}/Desktop/Sunpath/static/data/TransactionLog.pkl'.format(home)

df11.to_pickle(path11)
df12.to_pickle(path12)
