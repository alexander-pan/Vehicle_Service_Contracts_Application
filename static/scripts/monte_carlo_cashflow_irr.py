import numpy as np
import pandas as pd
import pyodbc
from datetime import datetime as dt, timedelta
from dateutil.relativedelta import *
from collections import OrderedDict
import sys
sys.path.append('../../passwords')
from sunpath_dbcreds import server,database,username,password

cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

#Making Initial Queries/Data
print 'Running Database SQL queries...'

#Query 1
query = """
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
DF = pd.read_sql(query,cnxn)
DF.drop_duplicates('PolicyNumber',inplace=True)

#Query 2
query = """
SELECT
FTL.PolicyNumber,FTL.TxDate, FTC.TxDescription,
FTL.TxAmount * FTC.PosOrNegTx AS NetTransaction
FROM dbo.SPF_Funding_Transaction_Log AS FTL
LEFT JOIN dbo.SPF_Funding_Transaction_Codes AS FTC
ON FTL.TxCode = FTC.TxCode
WHERE (FTL.CashTx = 1) AND (FTL.PolicyNumber IS NOT NULL)
ORDER BY FTL.PolicyNumber, FTL.TxDate;
"""
DF1 = pd.read_sql(query,cnxn)

#Query 3
query = """
select PolicyNumber,
PaymentsMade as InstallmentsPaid,
PaymentsRemaining as InstallmentsDue,
(PaymentsMade+PaymentsRemaining) as InstallmentTotal,
AmountFinanced as ContractualAmount,
CurrentInstallmentAmount as InstallmentAmount,
IsCancelled,
CancelDate,
LastPaymentDate
from dbo.daily_extract;
"""
DF2 = pd.read_sql(query,cnxn)

#Calculate Probabilities
#[installment total, installment paid]
P = np.zeros((25,25))
for i in range(1,25):
    dfn = DF.loc[(DF.Installments==i)]
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
    df = DF[DF.PolicyNumber==policy]
    eff_date = df.EffectiveDate.values[0]
    termdays = df.TermDays.values[0]
    seller_cost = df.SellerCost.values[0]
    num = (termdays + (eff_date - (due_date + relativedelta(days=30))).days)
    den = termdays
    ret_prem = (num/den * seller_cost) - 50
    return round(ret_prem,2)

#specifically creating cashflows for std dev values
def createCashFlows(policy,category,df):
    temp = df.loc[(df.PolicyNumber==policy)]

    #create rows for contract
    dfcopy = DFEmpty.copy()
    dfcopy = dfcopy.append({'PolicyNumber': policy,'Category': 'Cash Flow_%s' % category},ignore_index=True)
    dfcopy = dfcopy.append({'PolicyNumber': policy,'Category': category},ignore_index=True)

    #set contract specific variables
    installPaid = temp.PaymentsMadePast.values[0]
    installment_amt = temp.CurrentInstallmentAmount.values[0] - 4.0
    seller_adv = temp.SellerAdvanceAmount.values[0]
    admin_fee = temp.AdminPortionAmt.values[0]
    ins_reserve = temp.InsReservePortionAmt.values[0]
    term = temp.Installments.values[0]


    T0 = temp.EffectiveDate.values[0]
    T1 = (T0 + BDay(7)).date()
    T2 = (T0 + BDay(14)).date()
    T3 = (T0 + BDay(50)).date()

    #contract cancelled
    if category == 'Projected Receivable(-2std)':
        last_payment = cp[term][installPaid][3]
    elif category == 'Projected Receivable(-std)':
        last_payment = cp[term][installPaid][4]
    elif category == 'Projected Receivable(+std)':
        last_payment = cp[term][installPaid][5]
    elif category == 'Projected Receivable(+2std)':
        last_payment = cp[term][installPaid][6]

    if last_payment != term:
        cancel = True
    else:
        cancel = False

    #begin cashflows
    #making assumption that seller adv, admin fee, and ins reserve happen if first payment is made
    for i in range(1,last_payment+1):
        #Expected CashFlow
        if i == 1:
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow_%s'%category),str(T0)] = installment_amt
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow_%s'%category),str(T1)] = -seller_adv
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow_%s'%category),str(T2)] = -admin_fee
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow_%s'%category),str(T3)] = -ins_reserve
            t = T0
        elif (i-1) % 3 != 0:
            t = (t + BDay(29)).date()
            if t == T3:
                amount = installment_amt - ins_reserve
            else:
                amount = installment_amt
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow_%s'%category),str(t)] = amount
        elif (i-1) % 3 == 0:
            t = (t + BDay(31)).date()
            if t == T3:
                amount = installment_amt - ins_reserve
            else:
                amount = installment_amt
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow_%s'%category),str(t)] = amount

        if i == last_payment and cancel==True:
            if i == 1:
                due_date = (t + BDay(31)).date()
            elif (i+1)%3 != 0 :
                due_date = (t + BDay(29)).date()
            elif (i+1)%3 == 0 :
                due_date = (t + BDay(31)).date()
            returned_prem = getRP(policy,due_date)
            if due_date == T3:
                amount = returned_prem - ins_reserve
            else:
                amount = returned_prem
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow_%s'%category),str(due_date)] = amount

        #Get Projected Receivable
        if cancel:
            if i == 1:
                due_date = (t + BDay(31)).date()
            elif (i+1)%3 != 0 :
                due_date = (t + BDay(29)).date()
            elif (i+1)%3 == 0 :
                due_date = (t + BDay(31)).date()
            returned_prem = getRP(policy,due_date)
            projected = (last_payment - i) * installment_amt + returned_prem
        else:
            projected = (last_payment-i) * installment_amt
        dfcopy.loc[(dfcopy.PolicyNumber==policy)
                       & (dfcopy.Category==category),str(t)] = round(projected,2)
    return dfcopy

#Joining Tables
DF4 = DF1.merge(DF2[['PolicyNumber','IsCancelled','CancelDate','LastPaymentDate','InstallmentsPaid','InstallmentsDue','InstallmentTotal']],on='PolicyNumber')
DF5 = DF4.copy()
DF5['TxEndDate'] = DF5.apply(lambda x: x.TxDate + relativedelta(months=x.InstallmentsDue),axis=1)

#Creating Date Columns of all up to date dates
dates = (DF5['TxDate'].append(DF5['TxEndDate'])).unique()
dates = [x for x in sorted(dates)]
first = dates[0]
last = dates[-1]
current = first
columns = []
while current <= last:
    columns.append(str(current))
    current += relativedelta(days=1)
columns.insert(0,'Category')
columns.insert(0,'PolicyNumber')

#Calculate Cashflows table
print 'Calculating Cashflows...'
policies = DF5.PolicyNumber.unique().tolist()
DFEmpty = pd.DataFrame([],columns=columns)
DFCFlows = pd.DataFrame([],columns=columns)

for policy in policies:
    temp = DF4[DF4.PolicyNumber==policy]
    installPaid = temp.PaymentsMadePast.values[0]
    installment_amt = temp.CurrentInstallmentAmount.values[0] - 4.0
    seller_adv = temp.SellerAdvanceAmount.values[0]
    admin_fee = temp.AdminPortionAmt.values[0]
    ins_reserve = temp.InsReservePortionAmt.values[0]
    term = temp.Installments.values[0]

    T0 = temp.EffectiveDate.values[0]
    T1 = (T0 + BDay(7)).date()
    T2 = (T0 + BDay(14)).date()
    T3 = (T0 + BDay(50)).date()

    last_payment = cp[term][installPaid][2]

    if last_payment != term:
        cancel = True
    else:
        cancel = False

    dfcopy = DFEmpty.copy()
    dfcopy = dfcopy.append({'PolicyNumber': policy,'Category': 'Cash Flow(Expected)'},ignore_index=True)
    dfcopy = dfcopy.append({'PolicyNumber': policy,'Category': 'Gross Receivable'},ignore_index=True)
    dfcopy = dfcopy.append({'PolicyNumber': policy,'Category': 'Projected Receivable'},ignore_index=True)

    #begin cashflows
    #making assumption that seller adv, admin fee, and ins reserve happen if first payment is made
    for i in range(1,last_payment+1):
        #Expected CashFlow
        if i == 1:
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow(Expected)'),str(T0)] = installment_amt
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow(Expected)'),str(T1)] = -seller_adv
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow(Expected)'),str(T2)] = -admin_fee
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow(Expected)'),str(T3)] = -ins_reserve
            t = T0
        elif (i-1) % 3 != 0:
            t = (t + BDay(29)).date()
            if t == T3:
                amount = installment_amt - ins_reserve
            else:
                amount = installment_amt
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow(Expected)'),str(t)] = amount
        elif (i-1) % 3 == 0:
            t = (t + BDay(31)).date()
            if t == T3:
                amount = installment_amt - ins_reserve
            else:
                amount = installment_amt
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow(Expected)'),str(t)] = amount

        if i == last_payment and cancel==True:
            if i == 1:
                due_date = (t + BDay(31)).date()
            elif (i+1)%3 != 0 :
                due_date = (t + BDay(29)).date()
            elif (i+1)%3 == 0 :
                due_date = (t + BDay(31)).date()
            returned_prem = getRP(policy,due_date)
            if due_date == T3:
                amount = returned_prem - ins_reserve
            else:
                amount = returned_prem
            dfcopy.loc[(dfcopy.PolicyNumber==policy)
                           & (dfcopy.Category=='Cash Flow(Expected)'),str(due_date)] = amount

        #Get Gross Receivable
        gross = (term-i)*installment_amt
        dfcopy.loc[(dfcopy.PolicyNumber==policy)
                       & (dfcopy.Category=='Gross Receivable'),str(t)] = round(gross,2)

        #Get Projected Receivable
        if cancel:
            if i == 1:
                due_date = (t + BDay(31)).date()
            elif (i+1)%3 != 0 :
                due_date = (t + BDay(29)).date()
            elif (i+1)%3 == 0 :
                due_date = (t + BDay(31)).date()
            returned_prem = getRP(policy,due_date)
            projected = (last_payment - i) * installment_amt + returned_prem
        else:
            projected = (last_payment-i) * installment_amt
        dfcopy.loc[(dfcopy.PolicyNumber==policy)
                       & (dfcopy.Category=='Projected Receivable'),str(t)] = round(projected,2)
    DFCFlows = pd.concat([DFCFlows,dfcopy],sort=False)
    for category in ['Projected Receivable(-2std)','Projected Receivable(-std)','Projected Receivable(+std)','Projected Receivable(+2std)']:
        DFCFlows = pd.concat([DFCFlows,createCashFlows(policy,category,DF3)],sort=False)

print 'Saving Cashflows to file...'
DFCFlows.to_pickle('./data/Contract_Cashflows.pkl')

#Functions
def XIRR(transactions):
    years = [(ta[0] - transactions[0][0]).days / 365.0 for ta in transactions]
    residual = 1
    step = 0.01
    guess = .1
    epsilon = 0.0001
    limit = 10000
    while abs(residual) > epsilon and limit > 0:
        limit -= 1
        residual = 0.0
        for i, ta in enumerate(transactions):
            residual += ta[1] / pow(guess, years[i])
        if abs(residual) > epsilon:
            if residual > 0:
                guess += step
            else:
                guess -= step
                step /= 2.0
    return guess-1

def getIRR(policy,df1,df2,category,cols):
    tempDF = df1[cols].loc[(df1.PolicyNumber==policy) & (df1.Category==category)]
    tempDF = tempDF.loc[:,~tempDF.isnull().all()]
    all_dates = [dt.strptime(x,'%Y-%m-%d').date() for x in tempDF.columns[2:]]
    all_cashflows = zip(all_dates,tempDF.values[0][2:])
    term = df2[df2.PolicyNumber==policy].Installments.values[0]
    installPaid = df2[df2.PolicyNumber==policy].PaymentsMadePast.values[0]

    if category == 'Cash Flow(Expected)':
        last_payment = cp[term][installPaid][2]
    elif category == 'Cash Flow_Projected Receivable(-2std)':
        last_payment = cp[term][installPaid][3]
    elif category == 'Cash Flow_Projected Receivable(-std)':
        last_payment = cp[term][installPaid][4]
    elif category == 'Cash Flow_Projected Receivable(+std)':
        last_payment = cp[term][installPaid][5]
    elif category == 'Cash Flow_Projected Receivable(+2std)':
        last_payment = cp[term][installPaid][6]

    try:
        projectedIRR = XIRR(all_cashflows)
    except:
        projectedIRR = np.nan

    return projectedIRR,last_payment

def getActualIRR(policy,df):
    cashflows = []
    tempDF = df.loc[df.PolicyNumber==policy]
    txdates = tempDF.TxDate.unique().tolist()
    for date in txdates:
        amt = tempDF.loc[tempDF.TxDate==date].NetTransaction.sum()
        cashflows.append((date,amt))
    #print cashflows
    try:
        IRR = XIRR(cashflows)
    except:
        IRR = np.nan
    return IRR

#Compute IRR for individual contracts
