import numpy as np
import pandas as pd
import pyodbc
from datetime import datetime as dt, timedelta
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from collections import OrderedDict
import os
import sys

home = os.environ['HOME']
sys.path.append(home)
from sunpath_creds.dbcreds import server,database,username,password

linode = '/home/webapp/Sunpath/apps/'
sys.path.append('{0}{1}'.format(home,linode))

#local = '/Projects/Statusquota/sunpath/application/apps/'
#sys.path.append('{0}{1}'.format(home,local))
from controls import TXCODES,FUNDERS

cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

######################
#   Path Variables   #
######################

linode = '/home/webapp/Sunpath/static/data/'
#local = '/Projects/Statusquota/sunpath/application/static/data/'

#############################
#      Variables/Tables     #
#############################

path = '{0}{1}ExpectedValues.pkl'.format(home,linode)
#path = '{0}{1}ExpectedValues.pkl'.format(home,local)
DF_EXPVAL = pd.read_pickle(path)

#####################
#     Functions     #
#####################

def ExpectedValue(policy):
    return DF_EXPVAL.loc[DF_EXPVAL.PolicyNumber==policy].ExpectedValue.values[0]

def LastPayment(policy):
    return DF_EXPVAL.loc[DF_EXPVAL.PolicyNumber==policy].LastPaymentMade.values[0]

def buildCohortTable3(df):
    dataframe = df.copy()

    #cohort terms
    term_mix = ['1','2-6','7-12','13-15','16-18','19-24']
    table = []
    for terms in term_mix:
        table.append(getCohortRowStats3(dataframe,terms))
    columns = ['Installment Terms','Contracts Sold','Seller Advance',
               'Cancel Reserve','Cancel Reserve %',
               'Discount Amt', 'Discount Amt %',
               'Net Amount','Net Amount,Contract']
    result = pd.DataFrame(table,columns=columns)
    return result

def getCohortRowStats3(df,cohort):
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
        net_amt = round(calcNetHoldback(dataframe,'amount'))
        net_amt_contract = round(net_amt/N_contracts)
        row = (cohort,N_contracts,round(S),
               round(H),cancel_rsv,
               round(D),discount_amt,
               net_amt,net_amt_contract)
        return row

def calcNetHoldback(df1,output):
    #all completed, cancelled contracts
    holdback = []
    funder = []

    #dianositc
    df = df1.copy()
    SellerAdvance = df.AmountFinanced - (df.SellerCost + df.DiscountAmount + df.CancelReserveAmount)
    df['PreEndAmt'] = -(df.AdminPortionAmt + df.InsReservePortionAmt +
                     SellerAdvance + df.prorated_fee - df.total_install_rec)

    cancelled_completed = df.loc[(df.ContractStatus=='Cancelled') | (df.ContractStatus=='Completed')]
    opened = df.loc[df.ContractStatus=='Open']

    cancelled_completed = cancelled_completed.copy()
    opened = opened.copy()

    cancelled_completed['Deficit_Surplus'] = (cancelled_completed.PreEndAmt +
                                                cancelled_completed.end_contract_amt)
    opened['Deficit_Surplus'] = (opened.PreEndAmt +
                                   opened.ProjReceivable)

    df = pd.concat([opened,cancelled_completed],ignore_index=True,sort=False)

    if output=='amount':
        return df.Deficit_Surplus.sum().round()
    else:
        return "Error"

##################
#      SQL       #
##################

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
  and (de.PaymentsMade <= afd.installments)
  group by de.PolicyNumber,de.EffectiveDate,de.CancelDate,de.LastPaymentDate,de.IsCancelled,de.FundCo,sfd.SellerName;
"""
df1 = pd.read_sql(q1,cnxn)
df1.drop_duplicates('PolicyNumber',inplace=True)
path = '{0}{1}Scenario_Modeling_INFO.pkl'.format(home,linode)
#path = '{0}{1}Scenario_Modeling_INFO.pkl'.format(home,local)

print 'Saving to %s\n' % path
df1.to_pickle(path)

q2 = """
with funding_fees as (
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
)

select * from rates order by SellerName,Installments;
"""

DF_PER = pd.read_sql(q2,cnxn)
path = '{0}{1}Funding_Fee_Percents.pkl'.format(home,linode)
#path = '{0}{1}Funding_Fee_Percents.pkl'.format(home,local)
print 'Saving to %s\n' % path
DF_PER.to_pickle(path)

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
path = '{0}{1}TXLog_Cashflows.pkl'.format(home,linode)
#path = '{0}{1}TXLog_Cashflows.pkl'.format(home,local)
print 'Saving to %s\n' % path
df3.to_pickle(path)

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
  and (de.PaymentsMade <= afd.installments)
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
  CancelPercentage as rate
  --datediff(day,EffectiveDate,cancel_date) as day_utilized,
  --datediff(day,EffectiveDate,cancel_date)/TermDays as VUR
  from info
),

variables as (
  select t2.PolicyNumber,
  SellerName,
  case
    when IsCancelled=1 then 'Cancelled'
    when IsCancelled=0 and PaymentsMade < Installments then 'Open'
    else 'Completed'
  end as ContractStatus,
  IsCancelled,
  FundCo,
  Installments,
  CurrentInstallmentAmount,
  PaymentsMade,
  ReturnedPremium,
  DiscountAmount,
  CancelReserveAmount,
  SellerAdvanceAmount,
  AmountFinanced,
  PaymentsRemaining,
  EffectiveDate,
  InsReservePortionAmt,
  AdminPortionAmt,
  TermDays,
  SellerCost,
  case
    when IsCancelled=1 or PaymentsRemaining=0 then ReturnedPremium
    when IsCancelled=0 and PaymentsRemaining!=0 then null
    else 0.0
  end as end_contract_amt,
  round(rate*DiscountAmount,2) as prorated_fee,
  --day_utilized,
  --VUR,
  (case when IsCancelled=1 then rate else 0.0 end) as rate,
  --CurrentInstallmentAmount * Installments as payment_plan_amount,
  CurrentInstallmentAmount * PaymentsMade as total_install_rec,
  --round((1-VUR)*AdminPortionAmt,2) as Amt_Owed_SPF_PreFee,
  --round((1-VUR)*InsReservePortionAmt,2) as Amt_Owed_INS,
  round((DiscountAmount/(SellerAdvanceAmount+CancelReserveAmount))*100,2) as DiscountAmountPercent,
  round((CancelReserveAmount/(SellerAdvanceAmount+CancelReserveAmount))*100,2) as CancelReservePercent
  from scenario_info as t1 inner join calculations as t2
  on t1.PolicyNumber=t2.PolicyNumber
)

select * from variables;
"""
df4 = pd.read_sql(q4,cnxn)

#For Open Contracts, Get new ProratedFee
cancelled_completed = df4.loc[(df4.ContractStatus=='Cancelled') | (df4.ContractStatus=='Completed')]
opened = df4.loc[df4.ContractStatus=='Open']
opened = opened.copy()
cancelled_completed = cancelled_completed.copy()
cancelled_completed['ProjReceivable'] = 0
opened['ProjReceivable'] = [ExpectedValue(x) for x in opened.PolicyNumber]
cancelled_completed['LastPaymentMade'] = cancelled_completed.PaymentsMade
opened['LastPaymentMade'] = [LastPayment(x) for x in opened.PolicyNumber]

for i,row in opened.iterrows():
    #if contract is open,
    #I need to change the prorated fee,Day_utilized,VUR,AmtOwedSPF,AmtOwedIns
    seller = row.SellerName
    #day_utilized = row.day_utilized
    #add_day_utilized = row.LastPaymentMade-row.PaymentsMade

    #substitute until updated table from databse
    #adminportionamt = row.Amt_Owed_SPF_PreFee/(1-row.VUR)
    #insportionamt = row.Amt_Owed_INS/(1-row.VUR)

    #if we take the assumption that a payment is made every month, average 30 days
    #opened.at[i,'day_utilized'] = add_day_utilized*30 + day_utilized
    #opened.at[i,'VUR'] = opened.at[i,'day_utilized']/row.TermDays
    #opened.at[i,'Amt_Owed_SPF_PreFee'] = round((1-opened.at[i,'VUR'])*adminportionamt,2)
    #opened.at[i,'Amt_Owed_INS'] = round((1-opened.at[i,'VUR'])*insportionamt,2)
    #opened.at[i,'total_install_rec'] = round(row.LastPaymentMade*row.CurrentInstallmentAmount,2)
    opened.at[i,'rate'] = DF_PER.loc[(DF_PER.SellerName==seller) & (DF_PER.Installments==row.LastPaymentMade)].CancelPercentage.values[0]
    opened.at[i,'prorated_fee'] = round(opened.at[i,'rate']*row.DiscountAmount,2)
DF_NEW = pd.concat([opened,cancelled_completed],ignore_index=True,sort=False)

path = '{0}{1}Scenario_Modeling_Variable_INFO.pkl'.format(home,linode)
#path = '{0}{1}Scenario_Modeling_Variable_INFO.pkl'.format(home,local)
print 'Saving to %s\n' % path
DF_NEW.to_pickle(path)

final_result = buildCohortTable3(DF_NEW)
path = '{0}{1}SPF_AVERAGE.pkl'.format(home,linode)
#path = '{0}{1}SPF_AVERAGE.pkl'.format(home,local)

print 'Saving to %s\n' % path
final_result.to_pickle(path)
