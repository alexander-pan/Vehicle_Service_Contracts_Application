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

######################
#   Path Variables   #
######################
linode = '/Sunpath/static/data/'
#local = '/Projects/Statusquota/sunpath/application/static/data/'

#For Linode
path1 = '{0}{1}SPA_SPBankingStat.pkl'.format(home,linode)
path2 = '{0}{1}SPA_SPDepositsStat.pkl'.format(home,linode)
path3 = '{0}{1}SPA_SPPlugStat.pkl'.format(home,linode)
path4 = '{0}{1}SPA_SPPaymentsStat.pkl'.format(home,linode)
path5 = '{0}{1}SPA_Funded_Contracts.pkl'.format(home,linode)
path6 = '{0}{1}Banking_Transaction.pkl'.format(home,linode)
path7 = '{0}{1}Plug_Other.pkl'.format(home,linode)
path8 = '{0}{1}Payments.pkl'.format(home,linode)
path9 = '{0}{1}SPF_Premium.pkl'.format(home,linode)
path10 = '{0}{1}Deposits.pkl'.format(home,linode)
path11 = '{0}{1}SPA_FundingBankStat.pkl'.format(home,linode)
path12 = '{0}{1}TransactionLog.pkl'.format(home,linode)

#For Local
"""path1 = '{0}{1}SPA_SPBankingStat.pkl'.format(home,local)
path2 = '{0}{1}SPA_SPDepositsStat.pkl'.format(home,local)
path3 = '{0}{1}SPA_SPPlugStat.pkl'.format(home,local)
path4 = '{0}{1}SPA_SPPaymentsStat.pkl'.format(home,local)
path5 = '{0}{1}SPA_Funded_Contracts.pkl'.format(home,local)
path6 = '{0}{1}Banking_Transaction.pkl'.format(home,local)
path7 = '{0}{1}Plug_Other.pkl'.format(home,local)
path8 = '{0}{1}Payments.pkl'.format(home,local)
path9 = '{0}{1}SPF_Premium.pkl'.format(home,local)
path10 = '{0}{1}Deposits.pkl'.format(home,local)
path11 = '{0}{1}SPA_FundingBankStat.pkl'.format(home,local)
path12 = '{0}{1}TransactionLog.pkl'.format(home,local)"""

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

print 'Saving to %s\n' % path1
df1.to_pickle(path1)

print 'Saving to %s\n' % path2
df2.to_pickle(path2)

print 'Saving to %s\n' % path3
df3.to_pickle(path3)

print 'Saving to %s\n' % path4
df4.to_pickle(path4)

print 'Saving to %s\n' % path5
df5.to_pickle(path5)

print 'Saving to %s\n' % path6
df6.to_pickle(path6)

print 'Saving to %s\n' % path7
df7.to_pickle(path7)

print 'Saving to %s\n' % path8
df8.to_pickle(path8)

print 'Saving to %s\n' % path9
df9.to_pickle(path9)

print 'Saving to %s\n' % path10
df10.to_pickle(path10)

print 'Saving to %s\n' % path11
df11.to_pickle(path11)

print 'Saving to %s\n' % path12
df12.to_pickle(path12)
