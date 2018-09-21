import numpy as np
import pandas as pd
import pyodbc
from datetime import datetime as dt, timedelta
from dateutil.relativedelta import *
from pandas.tseries.offsets import *
from collections import OrderedDict
import sys
import os

home = os.environ['HOME']
sys.path.append(home)
from sunpath_creds.dbcreds import server,database,username,password

#sys.path.append('/home/webapp/Sunpath/apps')
sys.path.append('{0}/Desktop/Sunpath/apps/'.format(home))
from controls import TXCODES,FUNDERS
cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

#For App7,9,10
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
  CancelPercentage as rate,
  datediff(day,EffectiveDate,cancel_date) as day_utilized,
  datediff(day,EffectiveDate,cancel_date)/TermDays as VUR
  from info
),

variables as (
  select t2.PolicyNumber,SellerName,IsCancelled,FundCo,Installments,
  CurrentInstallmentAmount,PaymentsMade,ReturnedPremium,
  DiscountAmount,CancelReserveAmount,SellerAdvanceAmount,
  AmountFinanced,PaymentsRemaining,EffectiveDate,TermDays,SellerCost,
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

#Calculate Probabilities
#[installment total, installment paid]
P = np.zeros((25,25))
for i in range(1,25):
    dfn = df4.loc[(df4.Installments==i)]
    for j in range(0,25):
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

#Complete MC for N_term contract Installments
print 'Running MonteCarlo...'
N_term = range(1,25)
cp = {}

for N in N_term:
    cp[N] = []

for N in N_term:
    print '%d Term:' % N
    for i in range(0,N):
        X = []
        for k in range(10**5):
            j = i+1
            while j < N:
                k = np.random.choice([0,1],p=[P[N][j],1-P[N][j]])
                if k == 0:
                    j += 1
                else:
                    break
            X.append(j)

        mean = np.mean(X)
        std = np.std(X)
        expected = mean.round()

        #2 std above mean
        if mean+2*std > N:
            high2 = N
        else:
            high2 = (mean+2*std).round()

        #1 std from mean
        if mean+std > N:
            high = N
        else:
            high = (mean+std).round()

        #2 std below mean
        if mean-2*std < i+1:
            low2 = i+1
        else:
            low2 = (mean-2*std).round()

        #1 std below mean
        if mean-std < i+1:
            low = i+1
        else:
            low = (mean-std).round()

        #print 'payments %d: std: %d low2std:%d, low1std:%d, mean:%d, high1std:%d, high2std:%d' % (i,std,low2,low,expected,high,high2)
        cp[N].append((i,N,int(expected),int(low2),int(low),int(high),int(high2)))

#function to get returned premium using
#(policy,due_date)
def getRP(policy,due_date):
    df = df4[df4.PolicyNumber==policy]
    eff_date = df.EffectiveDate.values[0]
    termdays = df.TermDays.values[0]
    seller_cost = df.SellerCost.values[0]
    num = (termdays + (eff_date - (due_date + relativedelta(days=30))).days)
    den = termdays
    ret_prem = (num/den * seller_cost) - 50
    return round(ret_prem,2)

#specifically creating cashflows for std dev values
def getProjectedReceivable(row):
    #set contract specific variables
    policy = row.PolicyNumber
    installPaid = row.PaymentsMade
    installment_amt = row.CurrentInstallmentAmount - 4.0
    term = row.Installments


    T0 = row.EffectiveDate
    T1 = (T0 + BDay(7)).date()
    T2 = (T0 + BDay(14)).date()
    T3 = (T0 + BDay(50)).date()
    t = T0
    #contract cancelled
    #print policy,term,installPaid
    last_payment = cp[term][installPaid-1][5] #gets +std, this has the best precision from previous analysis

    if last_payment != term:
        cancel = True
    else:
        cancel = False

    #get t (time/date)
    #this is used to calculate due_date
    for i in range(installPaid+1,last_payment+1):
        if i == 1:
            t = T0
        elif (i-1) % 3 != 0:
            t = (t + BDay(29)).date()
        elif (i-1) % 3 == 0:
            t = (t + BDay(31)).date()

    #Get Projected Receivable
    if cancel:
        if i == 1:
            due_date = (t + BDay(31)).date()
        elif (i+1)%3 != 0 :
            due_date = (t + BDay(29)).date()
        elif (i+1)%3 == 0 :
            due_date = (t + BDay(31)).date()
        returned_prem = getRP(policy,due_date)
        projected = (last_payment - installPaid) * installment_amt + returned_prem
    else:
        projected = (last_payment - installPaid) * installment_amt
    return policy,projected

openedDF = df4.loc[df4.end_contract_amt.isnull()]
exp_df = []
for i,row in openedDF.iterrows():
    exp_df.append(getProjectedReceivable(row))
exp_val_df = pd.DataFrame(exp_df,columns=['PolicyNumber','ExpectedValue'])
#path = '{0}/Sunpath/static/data/ExpectedValues.pkl'.format(home)
path = '{0}/Desktop/Sunpath/static/data/ExpectedValues.pkl'.format(home)
exp_val_df.to_pickle(path)
