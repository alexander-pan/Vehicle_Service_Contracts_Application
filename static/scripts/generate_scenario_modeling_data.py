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
select distinct de.PolicyNumber,de.EffectiveDate,de.CancelDate,de.LastPaymentDate,de.IsCancelled,de.FundCo,sfd.SellerName,
  max(de.AmountFinanced) as AmountFinanced,max(sfd.TotalSalesPrice) as TotalSalePrice,max(de.DiscountAmount) as DiscountAmount,max(sfd.SellerCost) as SellerCost,
  max(CancelReserveAmount) as CancelReserveAmount,max(SellerAdvanceAmount) as SellerAdvanceAmount,
  max(AdminPortionAmt) as AdminPortionAmt,max(InsReservePortionAmt) as InsReservePortionAmt,
  max(de.CurrentInstallmentAmount) as CurrentInstallmentAmount,max(de.PaymentsMade) as PaymentsMade,max(de.PaymentsRemaining) as PaymentsRemaining,
  max(de.PaymentsMade+de.PaymentsRemaining) as Installments,
  max(sfd.DownPayment) as DownPayment,max(de.ReturnedPremium) as ReturnedPremium,max(de.ReturnedCommission) as ReturnedCommission,max(se.termmonths/12.0*365) as TermDays
  from dbo.daily_extract as de
  join  dbo.seller_funding_data as sfd on de.policynumber=sfd.policynumber
  join dbo.admin_funding_data as afd on de.policynumber=afd.policynumber
  join dbo.stoneeagle_all_customer_info as se on de.policynumber=se.policynumber
  where (de.PaymentsMade+de.PaymentsRemaining) = afd.installments
  group by de.PolicyNumber,de.EffectiveDate,de.CancelDate,de.LastPaymentDate,de.IsCancelled,de.FundCo,sfd.SellerName;
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

#Calculate Probability Distributions
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
df3 = pd.read_sql(q3,cnxn)
df3.to_pickle('../data/TXLog_Cashflows.pkl')

q4 = """
with scenario_info as (
  select distinct de.PolicyNumber,de.EffectiveDate,de.CancelDate,de.LastPaymentDate,de.IsCancelled,de.FundCo,sfd.SellerName,
  max(de.AmountFinanced) as AmountFinanced,max(sfd.TotalSalesPrice) as TotalSalePrice,max(de.DiscountAmount) as DiscountAmount,max(sfd.SellerCost) as SellerCost,
  max(CancelReserveAmount) as CancelReserveAmount,max(SellerAdvanceAmount) as SellerAdvanceAmount,
  max(AdminPortionAmt) as AdminPortionAmt,max(InsReservePortionAmt) as InsReservePortionAmt,
  max(de.CurrentInstallmentAmount) as CurrentInstallmentAmount,max(de.PaymentsMade) as PaymentsMade,max(de.PaymentsRemaining) as PaymentsRemaining,
  max(de.PaymentsMade+de.PaymentsRemaining) as Installments,
  max(sfd.DownPayment) as DownPayment,max(de.ReturnedPremium) as ReturnedPremium,max(de.ReturnedCommission) as ReturnedCommission,max(se.termmonths/12.0*365) as TermDays
  from dbo.daily_extract as de
  join  dbo.seller_funding_data as sfd on de.policynumber=sfd.policynumber
  join dbo.admin_funding_data as afd on de.policynumber=afd.policynumber
  join dbo.stoneeagle_all_customer_info as se on de.policynumber=se.policynumber
  where (de.PaymentsMade+de.PaymentsRemaining) = afd.installments
  group by de.PolicyNumber,de.EffectiveDate,de.CancelDate,de.LastPaymentDate,de.IsCancelled,de.FundCo,sfd.SellerName
),

funding_fees as (
  select SunPathAccountingCode, Installments,DiscountPercentage,CancelPercentage,FlatCancelFee,ReservePercentage
  from dbo.seller_info_funding_approved_partners as df1
  join dbo.seller_info_funding_parameters as df3 on df1.SunPathSellerCode=df3.SunPathSellerCode
),

txcodes as (
  select distinct SunPathSellerCode,SunPathAccountingCode,SellerName
  from dbo.daily_extract as t1
  join dbo.seller_funding_data as t2
  on t1.PolicyNumber = t2.PolicyNumber
  join dbo.seller_info_funding_approved_partners as t3
  on t1.sellercode = t3.paylinksellercode
),

rates as (
  select SellerName,
  Installments,
  CancelPercentage
  from funding_fees
  inner join txcodes on funding_fees.SunPathAccountingCode = txcodes.SunPathAccountingCode
),

info as (
  select PolicyNumber, CancelPercentage, EffectiveDate,TermDays,
  case
    when (CancelDate is null) then LastPaymentDate
    else CancelDate
  end as cancel_date
  from scenario_info as t1
  inner join rates as t2
  on t1.SellerName =t2.SellerName and t1.PaymentsMade = t2.Installments
),

calculations as (
  select PolicyNumber,
  CancelPercentage as rate,
  datediff(day,EffectiveDate,cancel_date) as day_utilized,
  datediff(day,EffectiveDate,cancel_date)/TermDays as VUR
  from info
),

variables as (
  select t2.PolicyNumber,SellerName,IsCancelled,FundCo,Installments,
  CurrentInstallmentAmount,PaymentsMade,ReturnedPremium,DiscountAmount,CancelReserveAmount,
  case
    when IsCancelled=1 or PaymentsRemaining=0 then ReturnedPremium
    when IsCancelled=0 and PaymentsRemaining!=0 then null
    else 0.0
  end as end_contract_amt,
  round(rate*DiscountAmount,2) as prorated_fee,
  day_utilized,VUR,(case when IsCancelled=1 then rate else 0.0 end) as rate,
  CurrentInstallmentAmount * Installments as payment_plan_amount,
  CurrentInstallmentAmount * PaymentsMade as total_install_rec,
  round((1-VUR)*AdminPortionAmt,2) as Amt_Owed_SPF_PreFee,
  round((1-VUR)*InsReservePortionAmt,2) as Amt_Owed_INS
  from scenario_info as t1 inner join calculations as t2
  on t1.PolicyNumber=t2.PolicyNumber
)

select * from variables;
"""
df4 = pd.read_sql(q4,cnxn)
df4.to_pickle('../data/Scenario_Modeling_Variable_INFO.pkl')

def ExpectedValueV2(N,j,amount,row):
    value = 0.0
    value2 = 0.0
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
        num = (row.TermDays + (row.EffectiveDate - (due_date+relativedelta(days=30))).days)
        den = row.TermDays
        RP = (num/den*row.SellerCost)-50
        #value = value + amount*p1 + RP*p2
        if N-i != 0:
            RP_i = RP*(N-i)/N
        else:
            RP_i = 0.0
        value = value + amount*p1 + RP_i*p2
        #print amount,p1,RP_i,p2, amount*p1 + RP_i*p2
        n += 1
    return row.PolicyNumber,round(value,2)

exp_df = []
for i,row in df1.iterrows():
    installments = row.PaymentsMade
    installAmt = row.CurrentInstallmentAmount
    term = row.Installments
    exp_df.append(ExpectedValueV2(term,installments,installAmt,row))
exp_val_df = pd.DataFrame(exp_df,columns=['PolicyNumber','ExpectedValue'])
exp_val_df.to_pickle('../data/ExpectedValues.pkl')
