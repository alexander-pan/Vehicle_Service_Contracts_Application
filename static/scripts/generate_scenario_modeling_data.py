import numpy as np
import pandas as pd
import pyodbc
from datetime import datetime as dt, timedelta
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from collections import OrderedDict
import sys
sys.path.append('../../../passwords')
from sunpath_dbcreds import server,database,username,password

sys.path.append('../../apps')
from controls import TXCODES,FUNDERS
cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

#For App7,9,10
q1 = """
select de.PolicyNumber, de.EffectiveDate,de.CancelDate,de.LastPaymentDate,de.IsCancelled,de.FundCo,sfd.SellerName,
de.AmountFinanced,sfd.TotalSalesPrice,de.DiscountAmount,sfd.SellerCost,
CancelReserveAmount,SellerAdvanceAmount,AdminPortionAmt,InsReservePortionAmt,
de.CurrentInstallmentAmount,de.PaymentsMade,de.PaymentsRemaining,de.PaymentsMade+de.PaymentsRemaining as Installments,
sfd.DownPayment,de.ReturnedPremium,de.ReturnedCommission,se.termmonths/12.0*365 as TermDays
from dbo.daily_extract as de
join  dbo.seller_funding_data as sfd on de.policynumber=sfd.policynumber
join dbo.admin_funding_data as afd on de.policynumber=afd.policynumber
join dbo.stoneeagle_all_customer_info as se on de.policynumber=se.policynumber
where (de.PaymentsMade+de.PaymentsRemaining) = afd.installments;
"""
df1 = pd.read_sql(q1,cnxn)
df1.drop_duplicates('PolicyNumber',inplace=True)
df1.to_pickle('../data/Scenario_Modeling_INFO.pkl')

q2 = """
select SunPathAccountingCode, Installments,DiscountPercentage,CancelPercentage,FlatCancelFee,ReservePercentage
from dbo.seller_info_funding_approved_partners as df1
join dbo.seller_info_funding_parameters as df3 on df1.SunPathSellerCode=df3.SunPathSellerCode;
"""

df2 = pd.read_sql(q2,cnxn)
df2.to_pickle('../data/Funding_Fee_Percents.pkl')

def buildCohortTable3(df,fee):
    dataframe = df.copy()

    #cohort terms
    term_mix = ['1','2-6','7-12','13-15','16-18','19-24']
    table = []
    for terms in term_mix:
        table.append(getCohortRowStats3(dataframe,fee,terms))
    columns = ['Installment Terms','Contracts Sold','Seller Advance',
               'Cancel Reserve','Cancel Reserve %',
               'Discount Amt', 'Discount Amt %',
               'Net Amount','Net Amount,Contract']
    result = pd.DataFrame(table,columns=columns)
    return result

def getCohortRowStats3(df,fee,cohort):
    dataframe = df.copy()
    if cohort == '1':
        dataframe = df.loc[df.Installments==1]
    elif cohort == '2-6':
        dataframe = df.loc[(df.Installments>=2) & (df.Installments<=6)]
    elif cohort == '7-12':
        dataframe = df.loc[(df.Installments>=7) & (df.Installments<=12)]
    elif cohort == '13-15':
        dataframe = df.loc[(df.Installments>=13) & (df.Installments<=15)]
    elif cohort == '16-18':
        dataframe = df.loc[(df.Installments>=16) & (df.Installments<=18)]
    elif cohort == '19-24':
        dataframe = df.loc[(df.Installments>=19) & (df.Installments<=24)]

    if not dataframe.empty:
        D = dataframe.DiscountAmount.mean()
        H = dataframe.CancelReserveAmount.mean()
        S = dataframe.SellerAdvanceAmount.mean()
        Z1 = (D/(H+S))*100
        Z2 = (H/(H+S))*100

        #values for row
        N_contracts = dataframe.shape[0]
        cancel_rsv = round(Z2,2)
        discount_amt = round(Z1,2)
        net_amt = round(calcNetHoldback(dataframe,df2,fee,'amount'))
        net_amt_contract = round(net_amt/N_contracts)
        row = (cohort,N_contracts,round(S),
               round(H),cancel_rsv,
               round(D),discount_amt,
               net_amt,net_amt_contract)
        return row

def calcNetHoldback(df1,df2,fee,output):
    #all completed, cancelled,open contracts
    holdback = []
    funder = []
    for i,row in df1.iterrows():
        installments = row.PaymentsMade
        funding_fee = row.DiscountAmount
        eff_date = row.EffectiveDate
        vendor = row.SellerName
        installAmt = row.CurrentInstallmentAmount
        term = row.Installments

        if row.CancelDate == None:
            cancel_date = row.LastPaymentDate
        else:
            cancel_date = row.CancelDate

        if row.IsCancelled == 1:
            rate = df2.loc[(df2['SunPathAccountingCode']==TXCODES[vendor][1])
                       & (df2['Installments']==installments)].CancelPercentage
        else:
            rate = 0.0

        day_utilized = (cancel_date-eff_date).days

        VUR = day_utilized/row.TermDays
        prorated_fee = float(rate * funding_fee)
        payment_plan_amount = installAmt * term
        cancel_reserve = row.CancelReserveAmount
        total_install_rec = installAmt * installments
        Amt_Owed_SPF = (1-VUR)*row.AdminPortionAmt-fee
        Amt_Owed_INS = (1-VUR)*row.InsReservePortionAmt
        deficit = cancel_reserve - payment_plan_amount + Amt_Owed_SPF + Amt_Owed_INS + funding_fee - prorated_fee + total_install_rec

        #if specific contract either cancelled, completed, or open
        if row.IsCancelled == 1 or row.PaymentsRemaining == 0:
            deficit = deficit + row.ReturnedPremium
        elif row.IsCancelled == 0 and row.PaymentsRemaining != 0:
            deficit = deficit + ExpectedValue(term,installments,installAmt,row)
        holdback.append(deficit)

    if output=='amount':
        return np.sum(holdback).round()
def ExpectedValue(N,j,amount,row):
    value = 0.0
    value2 = 0.0
    sellercost = row.SellerCost
    eff_date = row.EffectiveDate
    n = j-1
    if n % 3 == 0:
        prev_date = (row.LastPaymentDate + BDay(25)).date()
    else:
        prev_date = (row.LastPaymentDate + BDay(20)).date()
    for i in range(j+1,N+1):
        p1 = 1.0
        p2 = 1.0
        for k in range(j+1,i+1):
            p1 = p1 * P[N][k]

            if k != i:
                p2 = p2 * P[N][k]
            elif k == i:
                p2 = p2 * (1-P[N][k])

        #value = value + amount * P[N][i]
        if n % 3 == 0:
            due_date = (prev_date + BDay(25)).date()
        else:
            due_date = (prev_date + BDay(20)).date()
        prev_date = due_date

        #calculate returned premium
        num = (row.TermDays + (eff_date - (due_date+relativedelta(days=30))).days)
        den = row.TermDays
        RP = num/den*sellercost-50
        if row.Installments-i != 0:
            RP_i = RP/row.Installments#*(row.Installments-i)/row.Installments
        else:
            RP_i = 0.0
        value = value + amount*p1 + RP_i*p2
        #print amount*p1+ RP_i*p2
        n += 1
    return value

#[installment total, installment paid]
P = np.zeros((25,25))
for i in range(1,25):
    dfn = df1.loc[df1.Installments==i]
    for j in range(1,25):
        if i < j:
            pass
        else:
            A = dfn.loc[(dfn.IsCancelled==1)
                        & (dfn.PaymentsMade==j)].shape[0]
            B = dfn.loc[dfn.PaymentsMade>=j].shape[0]
            if B == 0:
                p = 0.0
            else:
                p = A*1.0/B
            P[i][j] = 1-p

final_result = buildCohortTable3(df1,50)
final_result.to_pickle('../data/SPF_AVERAGE.pkl')

q3 = """
SELECT
FTL.PolicyNumber,FTL.TxDate, FTC.TxDescription,
FTL.TxAmount * FTC.PosOrNegTx AS NetTransaction
FROM dbo.SPF_Funding_Transaction_Log AS FTL
LEFT JOIN dbo.SPF_Funding_Transaction_Codes AS FTC
ON FTL.TxCode = FTC.TxCode
WHERE (FTL.CashTx = 1) AND (FTL.PolicyNumber IS NOT NULL)
ORDER BY FTL.PolicyNumber, FTL.TxDate;
"""
df3 = pd.read_sql(q1,cnxn)
df3.to_pickle('../data/TXLog_Cashflows.pkl')