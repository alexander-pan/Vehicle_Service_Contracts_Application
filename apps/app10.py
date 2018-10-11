#import required libraries
from datetime import datetime as dt
from dateutil.relativedelta import *
import pandas as pd
import numpy as np
from flask import Flask
from flask_caching import Cache
from collections import OrderedDict
from pandas.tseries.offsets import *
#import pyodbc
import copy
from app import app
import time

import dash
from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import dash_table_experiments as dte

#Multi-Dropdown options
from controls import SELLERS, FUNDERS, TXCODES


#Get initial Dataframe to work with
DF = pd.read_pickle('./static/data/Scenario_Modeling_INFO.pkl')
DF_PER = pd.read_pickle('./static/data/Funding_Fee_Percents.pkl')
DF_SPFAVG = pd.read_pickle('./static/data/SPF_AVERAGE.pkl')
DF_EXPVAL = pd.read_pickle('./static/data/ExpectedValues.pkl')
DF_VAR = pd.read_pickle('./static/data/Scenario_Modeling_Variable_INFO.pkl')

#Setup App
app.config.suppress_callback_exceptions = True
app.css.append_css({"external_url":"../static/dashboard.css"})

CACHE_CONFIG = {
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'cache-directory'
}
cache = Cache()
cache.init_app(app.server,config=CACHE_CONFIG)

#Create controls
funder_options = [{'label': str(funder),
                   'value': FUNDERS[funder]}
                  for funder in FUNDERS]
seller_options = [{'label': str(seller),
                   'value': SELLERS[seller]}
                  for seller in SELLERS]
seller_options = sorted(seller_options)

#Calculate probability distribution
P = np.zeros((25,25))
for i in range(1,25):
    dfn = DF.loc[DF.Installments==i]
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

layout = dict(
    autosize=True,
    height=260,
    font=dict(color='#000000'),
    titlefont=dict(color='#000000', size='14'),
    #font=dict(color='#CCCCCC'),
    #titlefont=dict(color='#CCCCCC', size='14'),
    margin=dict(
        l=35,
        r=35,
        b=45,
        t=35
    ),
    hovermode='closest',
    #plot_bgcolor='#191A1A',
    #paper_bgcolor="#020202",
    legend=dict(font=dict(size=10), orientation='v'),
    title=''
)

#application layout
layout_page = html.Div([
    dcc.Link('Home Page',href='/'),
    html.Div(
        [
            html.H1('Scenario Tables & Outputs: Vendors',
                    style={'textAlign':'center'}),
        ],
    ),

    #Details allows the ability to hide selections
    html.Div([
        html.Details(
            [
                html.Summary(''),
                html.Div([
                    html.Label('Funder'),
                    dcc.Dropdown(
                        id='funder_10',
                        options=funder_options,
                        placeholder='Select a Funder',
                        multi=True
                    )
                ]),
            ],
            className='three columns',style={'display':'block'}
        ),

        html.Details(
            [
                html.Summary(''),
                html.Div([
                    html.Label('Seller'),
                    dcc.Dropdown(
                        id='seller_10',
                        options=seller_options,
                        placeholder='Select a Seller',
                        multi=True
                    )
                ]),
            ],
            className='six columns',style={'display':'block'}
        ),

#        html.Div(
#            [
#                html.Label('Contract Fee'),
#                    dcc.Input(
#                        id = 'fee',
#                        type='number',
#                        value=50
#                    ),
#            ],
#            className='three columns',style={'display':'block'}
#        ),
    ],style={'width':'100%','display':'inline-block'}),

    #First Table
    html.Div([
        html.Div(
            [
                dte.DataTable(
                    id='cohort_10',
                    rows=[{}],
                    columns=['Installment Terms',
                             'Contracts Sold',
                             '% Contracts Sold',
                             'Cancel Reserve %',
                             'Discount Amt %',
                             'Net Amount'],
                    sortable=False,
                    editable=False,
                    max_rows_in_viewport=7,
                )
            ],
        ),
    ],style={'width':'100%','display':'inline-block','margin':10}),

    #Second Table
    html.Div([
        html.Div(
            [
                dte.DataTable(
                    id='cohortT2_10',
                    rows=[{}],
                    columns=['Installment Terms',
                             'Cancel Reserve %',
                             'Discount Amt %'],
                    sortable=False,
                    editable=True,
                    max_rows_in_viewport=7
                )
            ],
            className='seven columns'
        ),

        html.Div(
            [
                dte.DataTable(
                    id='cohortT3_10',
                    rows=[{}],
                    columns=['Net Amount,Contract'],
                    sortable=False,
                    editable=False,
                )
            ],
            className='three columns'
        ),
    ],style={'width':'100%','display':'inline-block'}),

    #Stat Values
    html.Div([
        html.Div(
            [
                html.Div(
                    [
                        dcc.Textarea(
                            id='total_net_10',
                            value='0',
                            readOnly=True,
                            wrap=True,
                            style={'textAlign': 'center',
                                   'align': 'center',
                                   'fontSize':50,
                                   'color': '#CCCCCC',
                                   'backgroundColor': '#191A1A',
                                   'width': '100%',
                                   'margin':10
                                  }
                        ),
                        html.Label('Cumulative Net Deficit/Surplus',
                            style={'fontSize':15,
                                   'textAlign':'center'}),
                    ],
                    className='four columns'
                ),

                html.Div(
                    [
                        dcc.Textarea(
                            id='avg_contracts_month_10',
                            value='0',
                            readOnly=True,
                            wrap=True,
                            style={'textAlign': 'center',
                                   'align': 'center',
                                   'fontSize':50,
                                   'color': '#CCCCCC',
                                   'backgroundColor': '#191A1A',
                                   'width': '100%',
                                   'margin':10
                                  }
                        ),
                        html.Label('Avg. Contracts Sold, Month',
                            style={'fontSize':15,
                                   'textAlign':'center'}),
                    ],
                    className='four columns'
                ),
            ],
        ),
    ],style={'width':'100%','display':'inline-block'}),
    #Third Table
    html.Div([
        html.Div(
            [
                dte.DataTable(
                    id='cohortT4_10',
                    rows=[{}],
                    columns=['Installment Terms',
                             'Cancel Reserve %',
                             'Discount Amt %',
                             'Contracts,Month'],
                    sortable=False,
                    editable=True,
                    max_rows_in_viewport=7
                )
            ],
            className='seven columns'
        ),

        html.Div(
            [
                dte.DataTable(
                    id='cohortT5_10',
                    rows=[{}],
                    columns=['Net Amt,Contract','Accruing Net,Month'],
                    sortable=False,
                    editable=False,
                    max_rows_in_viewport=7
                )
            ],
            className='five columns'
        ),
    ],style={'width':'100%','display':'inline-block'}),
#ending of layout
],style={'margin-left':40,'margin-right':40})

#FUNCTIONS
"""This functions grabs the data based on seller and funder selectiions"""
"""Used In: all callbacks """
@cache.memoize()
def getCohort(df,seller,funder):
    dataframe = []
    for vendor in seller:
        for fundee in funder:
            dataframe.append(df.loc[(df.SellerName==vendor) & (df.FundCo==fundee)])
    return pd.concat(dataframe)

"""grabs all the contracts in the cohort range"""
"""Used In: getOutput(),getOutput2() """
@cache.memoize()
def getCohortSPFAVG(df,cohort):
    dataframe = df.copy()
    if cohort == '1':
        dataframe = dataframe.loc[dataframe.Installments==1]
    elif cohort == '2-6':
        dataframe = dataframe.loc[(dataframe.Installments>=2) & (dataframe.Installments<=6)]
    elif cohort == '7-12':
        dataframe = dataframe.loc[(dataframe.Installments>=7) & (dataframe.Installments<=12)]
    elif cohort == '13-15':
        dataframe = dataframe.loc[(dataframe.Installments>=13) & (dataframe.Installments<=15)]
    elif cohort == '16-18':
        dataframe = dataframe.loc[(dataframe.Installments>=16) & (dataframe.Installments<=18)]
    elif cohort == '19-24':
        dataframe = dataframe.loc[(dataframe.Installments>=19) & (dataframe.Installments<=24)]
    return dataframe

"""Calculates monthly sales volume"""
"""Used In: update_AvgContracts"""
@cache.memoize()
#Monthly Sales Volume
def MonthlySales(df,output):
    months = OrderedDict()
    if output == "mean":
        #count how many contracts there are in "month year"
        for date in sorted(df['EffectiveDate']):
            key = date.strftime('%b %y')
            if key in months:
                months[key] +=1
            else:
                months[key] = 1
        return int(np.mean(months.values()).round())
    elif output == 'mean_3months':
        #count how many contracts there are in "month year"
        for date in sorted(df['EffectiveDate']):
            key = date.strftime('%b %y')
            if key in months:
                months[key] +=1
            else:
                months[key] = 1
        return int(np.mean(months.values()[-3:]).round())

"""Looks for the Expected Value using contract policynumber, calculations done
beforehand"""
"""Used In: getEPRCohort(),calcNetHoldback(),calcNetHoldbackPerContract() """
#for N = Installment Term Total, j = how much has been paid currently,
#amount = current installment amount
@cache.memoize()
def ExpectedValue(policy):
    return DF_EXPVAL.loc[DF_EXPVAL.PolicyNumber==policy].ExpectedValue.values[0]

"""Gets the total net amount"""
"""Used In: update_NetDeficit"""
@cache.memoize()
def getTotalNetAmount(df):
    dataframe = df.copy()

    #cohort terms
    term_mix = ['1','2-6','7-12','13-15','16-18','19-24']
    net_amount = 0.0
    for terms in term_mix:
        net_amount = net_amount + getCohortNetAmount(dataframe,terms)
    return net_amount

"""gets cohort's net amount and calculates netholdback if contracts > 75"""
"""Used In: getTotalNetAmount()"""
@cache.memoize()
def getCohortNetAmount(df,cohort):
    dataframe = df.copy()
    if cohort == '1':
        dataframe = dataframe.loc[dataframe.Installments==1]
    elif cohort == '2-6':
        dataframe = dataframe.loc[(dataframe.Installments>=2) & (dataframe.Installments<=6)]
    elif cohort == '7-12':
        dataframe = dataframe.loc[(dataframe.Installments>=7) & (dataframe.Installments<=12)]
    elif cohort == '13-15':
        dataframe = dataframe.loc[(dataframe.Installments>=13) & (dataframe.Installments<=15)]
    elif cohort == '16-18':
        dataframe = dataframe.loc[(dataframe.Installments>=16) & (dataframe.Installments<=18)]
    elif cohort == '19-24':
        dataframe = dataframe.loc[(dataframe.Installments>=19) & (dataframe.Installments<=24)]

    if not dataframe.empty:
        if dataframe.shape[0] >= 75:
            net_amt = round(calcNetHoldback(dataframe,'amount'))
            return net_amt
        else:
            df1 = DF_SPFAVG.loc[DF_SPFAVG['Installment Terms']==cohort]
            N_contracts = dataframe.shape[0]
            net_amt_contract = df1['Net Amount,Contract'].values[0]
            net_amt = round(net_amt_contract * N_contracts)
            return net_amt
    else:
        return 0

"""Revised Function, Calculates NetHoldback, uses DF_VAR as core datatable"""
"""Used In: getCohortNetAmount(),getCohortRowStats(),getContracts()"""
@cache.memoize()
def calcNetHoldback(df1,output):
    #all completed, cancelled contracts
    holdback = []
    funder = []
    print "calculating..."
    start = time.time()

    df = df1.copy()
    SellerAdvance = df.AmountFinanced - (df.SellerCost + df.DiscountAmount + df.CancelReserveAmount)
    df['PreEndAmt'] = -(df.AdminPortionAmt + df.InsReservePortionAmt +
                     SellerAdvance + df.prorated_fee - df.total_install_rec)

    #we split by adding returned premium for cancelled/completed
    #or expected values for open contracts
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
        print "calculation complete: %f seconds" % (time.time() - start)
        return df.Deficit_Surplus.sum().round()
    else:
        return "Error"

"""Revised Function, Calculates the NetHoldback to be used for each contract"""
"""Used In: getOutput(),getOutput2()"""
@cache.memoize()
def calcNetHoldbackPerContract(df1,output,cancel_reserve,discount_amt):
    #all completed, cancelled contracts
    #Find Owed To Funder = Gross Capital + HldbckRsv + Porated Funding Fee - Total Installs Received
    holdback = []
    funder = []
    print "calculating..."
    start = time.time()

    df = df1.copy()
    df['prorated_fee'] = [round(float(x),2) for x in (df.rate * discount_amt)]
    SellerAdvance = df.AmountFinanced - (df.SellerCost + df.DiscountAmount + df.CancelReserveAmount)
    df['PreEndAmt'] = -(df.AdminPortionAmt + df.InsReservePortionAmt +
                     SellerAdvance + df.prorated_fee - df.total_install_rec)
    #we split by adding returned premium for cancelled/completed
    #or expected values for open contracts
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
        print "calculation complete: %f seconds" % (time.time() - start)
        return df.Deficit_Surplus.sum()
    else:
        return "Error"

"""Builds Main Summary, Cohort Tables"""
"""Used In: update_CohortTable"""
@cache.memoize()
def buildCohortTable(df):
    dataframe = df.copy()

    #cohort terms
    term_mix = ['1','2-6','7-12','13-15','16-18','19-24']
    table = []
    for terms in term_mix:
        table.append(getCohortRowStats(dataframe,terms))
    columns = ['Installment Terms','Contracts Sold',
               '% Contracts Sold','Cancel Reserve %',
               'Discount Amt %','Net Amount']
    result = pd.DataFrame(table,columns=columns)
    totals = pd.DataFrame([('Total',result['Contracts Sold'].sum(),result['% Contracts Sold'].sum(),
    round(result['Cancel Reserve %'].mean(),2),round(result['Discount Amt %'].mean(),2),
    round(result['Net Amount'].sum()))],columns=columns)
    final_result = result.append(totals)
    return final_result

"""Builds the editable table, inputs are cancel reserve and discount amt percentages"""
"""Used In: update_CohortTable2"""
@cache.memoize()
def buildCohortTable2(df):
    dataframe = df.copy()

    #cohort terms
    term_mix = ['1','2-6','7-12','13-15','16-18','19-24']
    table = []
    for terms in term_mix:
        table.append(getCohortRowStats2(dataframe,terms))
    columns = ['Installment Terms','Cancel Reserve %','Discount Amt %']
    result = pd.DataFrame(table,columns=columns)
    return result

"""Function gets the CohortRow stats
Used In: buildCohortTable()"""
@cache.memoize()
def getCohortRowStats(df,cohort):
    dataframe = df.copy()
    if cohort == '1':
        dataframe = dataframe.loc[dataframe.Installments==1]
    elif cohort == '2-6':
        dataframe = dataframe.loc[(dataframe.Installments>=2) & (dataframe.Installments<=6)]
    elif cohort == '7-12':
        dataframe = dataframe.loc[(dataframe.Installments>=7) & (dataframe.Installments<=12)]
    elif cohort == '13-15':
        dataframe = dataframe.loc[(dataframe.Installments>=13) & (dataframe.Installments<=15)]
    elif cohort == '16-18':
        dataframe = dataframe.loc[(dataframe.Installments>=16) & (dataframe.Installments<=18)]
    elif cohort == '19-24':
        dataframe = dataframe.loc[(dataframe.Installments>=19) & (dataframe.Installments<=24)]

    if not dataframe.empty:
        D = dataframe.DiscountAmount.mean()
        S = dataframe.SellerAdvanceAmount.mean()
        if dataframe.shape[0] >= 75:
            H = dataframe.CancelReserveAmount.mean()
            Z1 = (D/(H+S))*100
            Z2 = (H/(H+S))*100

            #values for row
            total = df.shape[0]
            N_contracts = dataframe.shape[0]
            contract_percent_sold = round(N_contracts*1.0/total*100,3)
            cancel_rsv = round(Z2,2)
            discount_amt = round(Z1,2)
            net_amt = round(calcNetHoldback(dataframe,'amount'))

            row = (cohort,N_contracts,contract_percent_sold,cancel_rsv,discount_amt,net_amt)
            return row
        else:
            df1 = DF_SPFAVG.loc[DF_SPFAVG['Installment Terms']==cohort]
            H = df1['Cancel Reserve'].values[0]
            #values for rows
            total = df.shape[0]
            N_contracts = dataframe.shape[0]
            contract_percent_sold = round(N_contracts*1.0/total*100,3)
            cancel_rsv = df1['Cancel Reserve %'].values[0]
            discount_amt =  round((D/(H+S))*100,2)
            net_amt_contract = df1['Net Amount,Contract'].values[0]
            net_amt = round(net_amt_contract * N_contracts)
            row = (cohort,N_contracts,contract_percent_sold,cancel_rsv,discount_amt,net_amt)
            return row
    else:
        return (cohort,0,0,0,0,0)

"""Function gets row stats """
"""Used In: buildCohortTable2"""
@cache.memoize()
def getCohortRowStats2(df,cohort):
    dataframe = df.copy()
    if cohort == '1':
        dataframe = dataframe.loc[dataframe.Installments==1]
    elif cohort == '2-6':
        dataframe = dataframe.loc[(dataframe.Installments>=2) & (dataframe.Installments<=6)]
    elif cohort == '7-12':
        dataframe = dataframe.loc[(dataframe.Installments>=7) & (dataframe.Installments<=12)]
    elif cohort == '13-15':
        dataframe = dataframe.loc[(dataframe.Installments>=13) & (dataframe.Installments<=15)]
    elif cohort == '16-18':
        dataframe = dataframe.loc[(dataframe.Installments>=16) & (dataframe.Installments<=18)]
    elif cohort == '19-24':
        dataframe = dataframe.loc[(dataframe.Installments>=19) & (dataframe.Installments<=24)]

    if not dataframe.empty:
        D = dataframe.DiscountAmount.mean()
        S = dataframe.SellerAdvanceAmount.mean()
        if dataframe.shape[0] >= 75:
            H = dataframe.CancelReserveAmount.mean()
            Z1 = (D/(H+S))*100
            Z2 = (H/(H+S))*100

            #values for row
            cancel_rsv = round(Z2,2)
            discount_amt = round(Z1,2)
            row = (cohort,cancel_rsv,discount_amt)
            return row
        else:
            df1 = DF_SPFAVG.loc[DF_SPFAVG['Installment Terms']==cohort]
            H = df1['Cancel Reserve'].values[0]
            #values for rows
            cancel_rsv = df1['Cancel Reserve %'].values[0]
            discount_amt = round((D/(H+S))*100,2)
            row = (cohort,cancel_rsv,discount_amt)
            return row
    else:
        return (cohort,0.0,0.0)


"""gets the contracts and rows to build editable table 2 """
"""Used In: buildCohortTableOutput2"""
@cache.memoize()
def getContracts(df,cohort):
    dataframe = df.copy()
    if cohort == '1':
        dataframe = dataframe.loc[dataframe.Installments==1]
    elif cohort == '2-6':
        dataframe = dataframe.loc[(dataframe.Installments>=2) & (dataframe.Installments<=6)]
    elif cohort == '7-12':
        dataframe = dataframe.loc[(dataframe.Installments>=7) & (dataframe.Installments<=12)]
    elif cohort == '13-15':
        dataframe = dataframe.loc[(dataframe.Installments>=13) & (dataframe.Installments<=15)]
    elif cohort == '16-18':
        dataframe = dataframe.loc[(dataframe.Installments>=16) & (dataframe.Installments<=18)]
    elif cohort == '19-24':
        dataframe = dataframe.loc[(dataframe.Installments>=19) & (dataframe.Installments<=24)]


    if not dataframe.empty:
        AF = dataframe.AmountFinanced.mean()
        D = dataframe.DiscountAmount.mean()
        S = dataframe.SellerAdvanceAmount.mean()
        if dataframe.shape[0] >= 75:
            H = dataframe.CancelReserveAmount.mean()
            Z1 = (H/(H+S))
            Z2 = (D/(H+S))
            Z = Z1/AF*(H+S) + Z2/AF*(H+S)
            net_amt = calcNetHoldback(dataframe,'amount')
            #print net_amt, Z*AF
            N = (abs(net_amt)/(Z*AF)/12).round()

            #values for row
            cancel_rsv = round(Z1*100.0,2)
            discount_amt = round(Z2*100.0,2)
            row = (cohort,cancel_rsv,discount_amt,N)
            return row
        else:
            df1 = DF_SPFAVG.loc[DF_SPFAVG['Installment Terms']==cohort]

            #values for rows
            total = df.shape[0]
            N_contracts = dataframe.shape[0]
            contract_percent_sold = round(N_contracts*1.0/total*100,3)
            H = df1['Cancel Reserve'].values[0]
            cancel_rsv = df1['Cancel Reserve %'].values[0]
            discount_amt = round((D/(H+S))*100,2)
            net_amt_contract = df1['Net Amount,Contract'].values[0]
            net_amt = round(net_amt_contract * N_contracts)
            N = (abs(net_amt)/(D+H)/12).round()
            row = (cohort,cancel_rsv,discount_amt,N)
            return row
    else:
        return (cohort,0.0,0.0,0)

"""Builds the 2nd editable table, inputs are cancel reserve, discount amt percentages
and contracts per month"""
"""Used In: update_CohortTable4"""
@cache.memoize()
def buildCohortTableOutput2(df):
    dataframe = df.copy()
    term_mix = ['1','2-6','7-12','13-15','16-18','19-24']
    table = []
    table2 = []
    for terms in term_mix:
        table.append(getContracts(dataframe,terms))
    columns = ['Installment Terms','Cancel Reserve %','Discount Amt %','Contracts,Month']
    result = pd.DataFrame(table,columns=columns)
    return result

"""Outputs the results of the live data from cohort table 2"""
"""Used In: update_CohortTable3"""
@cache.memoize()
def getOutput(df,dff):
    table = []
    for i,row in dff.iterrows():
        dataframe = df.copy()
        cohort = row["Installment Terms"]

        if cohort == '1':
            dataframe = dataframe.loc[dataframe.Installments==1]
        elif cohort == '2-6':
            dataframe = dataframe.loc[(dataframe.Installments>=2) & (dataframe.Installments<=6)]
        elif cohort == '7-12':
            dataframe = dataframe.loc[(dataframe.Installments>=7) & (dataframe.Installments<=12)]
        elif cohort == '13-15':
            dataframe = dataframe.loc[(dataframe.Installments>=13) & (dataframe.Installments<=15)]
        elif cohort == '16-18':
            dataframe = dataframe.loc[(dataframe.Installments>=16) & (dataframe.Installments<=18)]
        elif cohort == '19-24':
            dataframe = dataframe.loc[(dataframe.Installments>=19) & (dataframe.Installments<=24)]

        N_contracts = dataframe.shape[0]
        D = row['Discount Amt %']
        H = row['Cancel Reserve %']
        if (float(H) == 0) and (float(D) == 0):
            net_per_contract = 0
        else:
            if N_contracts < 75:
                df1 = DF_SPFAVG.loc[DF_SPFAVG['Installment Terms']==cohort]
                const = df1['Seller Advance'].values[0] + df1['Cancel Reserve'].values[0]
                cohortDF = getCohortSPFAVG(DF_VAR,cohort)
                D = round(float(D)*const/100.0,2)
                H = round(float(H)*const/100.0,2)
                net_amt = calcNetHoldbackPerContract(cohortDF,'amount',H,D)
                net_per_contract = round(net_amt/cohortDF.shape[0])
            else:
                const = dataframe.CancelReserveAmount.mean() + dataframe.SellerAdvanceAmount.mean()
                D = round(float(D)*const/100.0,2)
                H = round(float(H)*const/100.0,2)
                net_amt = calcNetHoldbackPerContract(dataframe,'amount',H,D)
                net_per_contract = round(net_amt/N_contracts)
        table.append((cohort,net_per_contract))
    return pd.DataFrame(table,columns=['Installment Terms','Net Amount,Contract'])

"""Outputs the results of the live data from cohort table 4"""
"""Used In: update_CohortTable5"""
@cache.memoize()
def getOutput2(df,dff):
    table = []
    for i,row in dff.iterrows():
        dataframe = df.copy()
        cohort = row["Installment Terms"]
        if cohort == '1':
            dataframe = dataframe.loc[dataframe.Installments==1]
        elif cohort == '2-6':
            dataframe = dataframe.loc[(dataframe.Installments>=2) & (dataframe.Installments<=6)]
        elif cohort == '7-12':
            dataframe = dataframe.loc[(dataframe.Installments>=7) & (dataframe.Installments<=12)]
        elif cohort == '13-15':
            dataframe = dataframe.loc[(dataframe.Installments>=13) & (dataframe.Installments<=15)]
        elif cohort == '16-18':
            dataframe = dataframe.loc[(dataframe.Installments>=16) & (dataframe.Installments<=18)]
        elif cohort == '19-24':
            dataframe = dataframe.loc[(dataframe.Installments>=19) & (dataframe.Installments<=24)]

        N_contracts = dataframe.shape[0]
        D = row['Discount Amt %']
        H = row['Cancel Reserve %']
        N = row['Contracts,Month']
        if (float(H) == 0) and (float(D) == 0):
            net_per_contract = 0
            accuring = int(N) * net_per_contract
        else:
            if N_contracts < 75:
                df1 = DF_SPFAVG.loc[DF_SPFAVG['Installment Terms']==cohort]
                const = df1['Seller Advance'].values[0] + df1['Cancel Reserve'].values[0]
                cohortDF = getCohortSPFAVG(DF_VAR,cohort)
                D = round(float(D)*const/100.0,2)
                H = round(float(H)*const/100.0,2)
                net_amt = calcNetHoldbackPerContract(cohortDF,'amount',H,D)
                net_per_contract = round(net_amt/cohortDF.shape[0])
            else:
                const = dataframe.CancelReserveAmount.mean() + dataframe.SellerAdvanceAmount.mean()
                D = round(float(D)*const/100.0,2)
                H = round(float(H)*const/100.0,2)
                net_amt = calcNetHoldbackPerContract(dataframe,'amount',H,D)
                net_per_contract = round(net_amt/N_contracts)
            accuring = round(int(N) * net_per_contract)
        table.append((cohort,net_per_contract,accuring))
    return pd.DataFrame(table,columns=['Installment Terms','Net Amt,Contract','Accruing Net,Month'])

#callbacks to update values in layout 2
@app.callback(Output('total_net_10','value'),
             [Input('funder_10','value'),
              Input('seller_10','value')])
def update_NetDeficit(funder,seller):
    if ((funder is not None) and (seller is not None)):
        dataframe = getCohort(DF_VAR,seller,funder)
        hldbckAmt = getTotalNetAmount(dataframe)
        if hldbckAmt < 0:
            amount = '-${:,.0f}'.format(abs(hldbckAmt))
        else:
            amount = '${:,.0f}'.format(abs(hldbckAmt))
        return '%s' % amount

@app.callback(Output('avg_contracts_month_10','value'),
               [Input('funder_10','value'),
               Input('seller_10','value')])
def update_AvgContracts(funder,seller):
    if ((funder is not None) and (seller is not None)):
        dataframe = getCohort(DF,seller,funder)
        return MonthlySales(dataframe,'mean')

@app.callback(Output('cohort_10','rows'),
             [Input('funder_10','value'),
              Input('seller_10','value')])
def update_CohortTable(funder,seller):
    if ((funder is not None) and (seller is not None)):
        #core dataframe
        dataframe = getCohort(DF_VAR,seller,funder)
        final_result = buildCohortTable(dataframe)
        return final_result.to_dict('records',into=OrderedDict)

@app.callback(Output('cohortT2_10','rows'),
             [Input('funder_10','value'),
              Input('seller_10','value')])
def update_CohortTable2(funder,seller):
    if ((funder is not None) and (seller is not None)):
        #core dataframe
        dataframe = getCohort(DF_VAR,seller,funder)
        result = buildCohortTable2(dataframe)
        columns = ['Installment Terms','Cancel Reserve %','Discount Amt %']
        return result[columns].to_dict('records',into=OrderedDict)

@app.callback(Output('cohortT3_10','rows'),
             [Input('funder_10','value'),
              Input('seller_10','value'),
              Input('cohortT2_10','rows')])
def update_CohortTable3(funder,seller,rows):
    if ((funder is not None) and (seller is not None)):
        #core dataframe
        dff = pd.DataFrame(rows)
        dataframe = getCohort(DF_VAR,seller,funder)
        result = getOutput(dataframe,dff)
        return result.to_dict('records',into=OrderedDict)
    else:
        return pd.DataFrame().to_dict('records')

@app.callback(Output('cohortT4_10','rows'),
             [Input('funder_10','value'),
              Input('seller_10','value')])
def update_CohortTable4(funder,seller):
    if ((funder is not None) and (seller is not None)):
        #core dataframe
        dataframe = getCohort(DF_VAR,seller,funder)

        result = buildCohortTableOutput2(dataframe)
        return result.to_dict('records',into=OrderedDict)

@app.callback(Output('cohortT5_10','rows'),
             [Input('funder_10','value'),
              Input('seller_10','value'),
              Input('cohortT4_10','rows')])
def update_CohortTable5(funder,seller,rows):
    if ((funder is not None) and (seller is not None)):
        #core dataframe
        dff = pd.DataFrame(rows)
        dataframe = getCohort(DF_VAR,seller,funder)
        result = getOutput2(dataframe,dff)
        return result.to_dict('records',into=OrderedDict)
    else:
        return pd.DataFrame().to_dict('records')
