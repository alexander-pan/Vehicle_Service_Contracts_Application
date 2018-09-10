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
df1.to_pickle('../data/SPA_SPBankingStat.pkl')
df2.to_pickle('../data/SPA_SPDepositsStat.pkl')
df3.to_pickle('../data/SPA_SPPlugStat.pkl')
df4.to_pickle('../data/SPA_SPPaymentsStat.pkl')
df5.to_pickle('../data/SPA_Funded_Contracts.pkl')

#App4 Data Tables
df6.to_pickle('../data/Banking_Transaction.pkl')
df7.to_pickle('../data/Plug_Other.pkl')
df8.to_pickle('../data/Payments.pkl')
df9.to_pickle('../data/SPF_Premium.pkl')
df10.to_pickle('../data/Deposits.pkl')

#App5,6 DataTables
df11.to_pickle('../data/SPA_FundingBankStat.pkl')
df12.to_pickle('../data/TransactionLog.pkl')
