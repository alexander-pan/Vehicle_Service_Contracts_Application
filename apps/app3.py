#import required libraries
from datetime import datetime as dt
from dateutil.relativedelta import *
import pandas as pd
import numpy as np
from flask import Flask
from flask_caching import Cache
from collections import OrderedDict
#import pyodbc
import copy
from app import app

import dash
from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import dash_table_experiments as dte

#Get initial Dataframe to work with
df1 = pd.read_pickle('./static/data/SPA_SPBankingStat.pkl')
df2 = pd.read_pickle('./static/data/SPA_SPDepositsStat.pkl')
df3 = pd.read_pickle('./static/data/SPA_Funded_Contracts.pkl')
df4 = pd.read_pickle('./static/data/SPA_SPPlugStat.pkl')
df5 = pd.read_pickle('./static/data/SPA_SPPaymentsStat.pkl')


#Setup App
app.config.suppress_callback_exceptions = True
app.css.append_css({"external_url":"../static/dashboard.css"})

CACHE_CONFIG = {
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'cache-directory'
}
cache = Cache()
cache.init_app(app.server,config=CACHE_CONFIG)

#Application Layout
layout_page = html.Div([
    dcc.Link('Home Page',href='/'),
    html.Div([
        html.Div(
            [
                html.H1('Bank Reconcillation Admin Dashboard',
                        style={'textAlign': 'center'}),
            ],
            className='row'
        ),
        html.Div(
            [
                html.Label('Date Range for Clear Date'),
                dcc.DatePickerRange(
                        id = 'date_range',
                        min_date_allowed=dt(2018,1,1),
                        max_date_allowed=dt.now(),
                        initial_visible_month=dt(2018,1,1),
                        number_of_months_shown=2,
                        end_date=dt(2018,1,31).date(),#dt.now().date(),
                        start_date=dt(2018,1,1).date(),
                        minimum_nights=0,
                ),
                dcc.RadioItems(
                    id = 'viewtype',
                    options=[
                        {'label':'Days','value':'days'},
                        {'label':'Months','value':'months'}
                    ],
                    value='days',
                    labelStyle={'display':'inline-block'}
                ),
            ],
            className='five columns'
        ),
    ],className='row'),

    #First Table
    html.Div(
        [
            html.Div(
                [
                    html.H4('Bank of America'),
                    dte.DataTable(
                            id='bank_table',
                            rows=[{}],
                            columns=['Days',
                                     'Ops',
                                     'Claims',
                                     'INS',
                                     'Payroll',
                                     'Escrow',
                                     'Total'],
                            sortable=True,
                            editable=False
                    ),
                ],
                #className='five columns'
            ),
        ],
        className='row'
    ),

    #Second Table
    html.Div(
        [
            html.Div(
                [
                    html.H4('Sunpath'),
                    dte.DataTable(
                            id='sunpath_table',
                            rows=[{}],
                            columns=['Days',
                                     'Deposits',
                                     'Insurance',
                                     'Plugs',
                                     'Packs Payments',
                                     'Total',
                                     'Variance'],
                            sortable=True,
                            editable=False
                    ),
                ],
                #className='five columns'
            ),
        ],
        className='row'
    ),
])

@cache.memoize()
def buildDayRange(start_date,end_date):
    delta = end_date - start_date
    return range(start_date.day,delta.days+1)
    #return range(start_date.day,end_date.day+1)

@cache.memoize()
def convertDatetime(DF,col):
    df = DF.copy()
    df[col] = pd.to_datetime(df[col],format="%m/%d/%y")
    df[col] = df[col].apply(lambda x: x.date())
    return df[col]

@cache.memoize()
def buildBankTable(DF,start_date,end_date,type_date):
    df = DF.copy()
    accounts = df.Account.unique().tolist()
    if type_date == 'months':
        df['Date'] = pd.to_datetime(df['ClearDate'],format='%m/%d/%y')

    df['ClearDate'] = convertDatetime(df,'ClearDate')
    df = df.loc[(df['ClearDate']>=start_date) & (df['ClearDate']<=end_date)]
    dates = sorted(list(set(df.ClearDate)))

    if type_date == 'days':
        dayDates = [x.day for x in dates]
        dictDates = dict([(x.day,x) for x in dates])

        #build days
        days = buildDayRange(start_date,end_date)
        table = []
        for day in days:
            if day in dayDates:
                acc_amts = []
                for acc in accounts:
                    df_acc = df.loc[df['Account']==acc]
                    date = dictDates[day]
                    temp = df_acc.loc[df_acc['ClearDate']==date]
                    amt = temp.Amount.sum()
                    acc_amts.append(amt)

                table.append((day,
                              acc_amts[0],
                              acc_amts[1],
                              acc_amts[2],
                              acc_amts[3],
                              acc_amts[4],
                              sum(acc_amts)))
            else:
                table.append((day,np.nan))
        accounts.insert(0,'Days')
    else:
        monthDates = set([x.strftime('%B') for x in dates])
        table=[]
        for month in monthDates:
            acc_amts = []
            for acc in accounts:
                df_acc = df.loc[df['Account']==acc]
                month_amt = df_acc.groupby(df_acc['Date'].dt.strftime('%B'))['Amount'].sum()[month]
                acc_amts.append(month_amt)
            table.append((month,
                         acc_amts[0],
                         acc_amts[1],
                         acc_amts[2],
                         acc_amts[3],
                         acc_amts[4],
                         sum(acc_amts)))
        accounts.insert(0,'Month')
    accounts.append('Total')
    dataframe = pd.DataFrame(table,columns=accounts)
    dataframe.fillna(0,inplace=True)
    return dataframe.round(2)

#build Sunpath tables for datetime cleardate objects
#Deposit and Ins Tables
@cache.memoize()
def buildTableV1(DF,col_name,start_date,end_date,viewtype):
    df = DF.copy()
    df = df.loc[(df['ClearDate']>=start_date) & (df['ClearDate']<=end_date)]
    dates = sorted(list(set(df.ClearDate)))

    if viewtype == 'days':
        dayDates = [x.day for x in dates]
        dictDates = dict([(x.day,x) for x in dates])

        #build days
        days = buildDayRange(start_date,end_date)
        table = []

        for day in days:
            if day in dayDates:
                date = dictDates[day]
                temp = df.loc[df['ClearDate']==date]
                amt = 0
                if col_name == 'Deposits':
                    amt = temp.PaymentAmount.sum()
                elif col_name == 'Insurance':
                    amt = temp.InsuranceReserve.sum()
                table.append((day,amt))
            else:
                table.append((day,np.nan))
        cols = ['Days',col_name]
    else:
        df['Date'] = df['ClearDate'].apply(lambda x: x.strftime('%B'))
        monthDates = set([x.strftime('%B') for x in dates])
        table=[]
        for month in monthDates:
            temp  = df.loc[df['Date']==month]
            amt = 0
            if col_name == 'Deposits':
                amt = temp.PaymentAmount.sum()
            elif col_name == 'Insurance':
                amt = temp.InsuranceReserve.sum()
            table.append((month,amt))
        cols = ['Month',col_name]
    return pd.DataFrame(table,columns=cols)

@cache.memoize()
def buildTableV2(DF,col_name,start_date,end_date,viewtype):
    df = DF.copy()

    if viewtype == 'months':
        df['Date'] = pd.to_datetime(df['ClearDate'],format='%m/%d/%y')

    df['ClearDate'] = convertDatetime(df,'ClearDate')
    df = df.loc[(df['ClearDate']>=start_date) & (df['ClearDate']<=end_date)]
    dates = sorted(list(set(df.ClearDate)))

    if viewtype == 'days':
        dayDates = [x.day for x in dates]
        dictDates = dict([(x.day,x) for x in dates])

        #build days
        days = buildDayRange(start_date,end_date)
        table = []

        for day in days:
            if day in dayDates:
                date = dictDates[day]
                temp = df.loc[df['ClearDate']==date]
                amt = 0
                if col_name == 'Plugs':
                    amt = temp.Amount.sum() #change this value
                table.append((day,amt))
            else:
                table.append((day,np.nan))

        cols = ['Days',col_name] #Change this col name
    else:
        monthDates = set([x.strftime('%B') for x in dates])
        table = []

        for month in monthDates:
            amt = 0
            temp = df.loc[df['Date'].dt.strftime('%B')==month]
            if col_name == 'Plugs':
                amt = temp.Amount.sum()
            table.append((month,amt))
        cols = ['Month',col_name]
    return pd.DataFrame(table,columns=cols)

#Build Packs Payments
@cache.memoize()
def buildPackTable(DF1,DF2,start_date,end_date,viewtype):
    dataframe = DF1.loc[(DF1['ClearDate']>=start_date) & (DF1['ClearDate']<=end_date)]
    dates = sorted(list(set(dataframe.ClearDate)))

    if viewtype == 'days':
        dayDates = [x.day for x in dates]
        dictDates = dict([(x.day,x) for x in dates])

        #build days
        days = buildDayRange(start_date,end_date)
        table = []

        for day in days:
            if day in dayDates:
                date = dictDates[day]
                temp = DF1.loc[DF1['ClearDate']==date]
                temp2 = DF2.loc[DF2['ClearDate']==date]
                pack_amt = temp.PaymentAmount.sum() + temp2.Amount.sum()
                table.append((day,pack_amt))
            else:
                table.append((day,np.nan))
        cols = ['Days','Packs Payments']
    else:
        df_1 = dataframe.copy()
        df_2 = DF2.copy()
        df_2['Date'] = pd.to_datetime(df_2['ClearDate'],format='%m/%d/%y')
        df_2['ClearDate'] = convertDatetime(df_2,'ClearDate')
        df_2 = df_2.loc[(df_2['ClearDate']>=start_date)
                       & (df_2['ClearDate']<=end_date)]
        df_1['Date'] = df_1['ClearDate'].apply(lambda x: x.strftime('%B'))
        monthDates = set([x.strftime('%B') for x in dates])
        table = []
        for month in monthDates:
            temp = df_1.loc[df_1['Date']==month]
            temp2 = df_2.loc[df_2['Date'].dt.strftime('%B')==month]
            pack_amt = temp.PaymentAmount.sum() + temp2.Amount.sum()
            table.append((month,pack_amt))
        cols = ['Month','Packs Payments']
    return pd.DataFrame(table,columns=cols)

@cache.memoize()
def buildTableSunpath(start_date,end_date,viewtype):
    depDF = buildTableV1(df2,'Deposits',start_date,end_date,viewtype)
    insDF = buildTableV1(df3,'Insurance',start_date,end_date,viewtype)
    plugsDF = buildTableV2(df4,'Plugs',start_date,end_date,viewtype)
    packsDF = buildPackTable(df5,df4.loc[df4.Type=='Pack'],start_date,end_date,viewtype)

    if viewtype == 'days':
        DF = depDF.merge(insDF,on='Days')
        DF = DF.merge(plugsDF,on='Days')
        DF = DF.merge(packsDF,on='Days')
    else:
        DF = depDF.merge(insDF,on='Month')
        DF = DF.merge(plugsDF,on='Month')
        DF = DF.merge(packsDF,on='Month')

    cols = ['Deposits','Insurance','Plugs','Packs Payments']
    DF['Total'] = DF[cols].sum(axis=1,skipna=True)

    #Build variance
    bankDF = buildBankTable(df1,start_date,end_date,viewtype)

    if viewtype == 'days':
        dataframe = bankDF.merge(DF,on='Days')
    else:
        dataframe = bankDF.merge(DF,on='Month')
    DF['Variance'] = dataframe['Total_x'] - dataframe['Total_y']
    DF.fillna(0,inplace=True)
    return DF.round(2)

@app.callback(Output('bank_table','columns'),
               [Input('viewtype','value')])
def updateColumns(viewtype):
    if viewtype == 'days':
        cols =['Days',
               'Ops',
               'Claims',
               'INS',
               'Payroll',
               'Escrow',
               'Total']
        return cols
    else:
        cols =['Month',
               'Ops',
               'Claims',
               'INS',
               'Payroll',
               'Escrow',
               'Total']
        return cols

@app.callback(Output('sunpath_table','columns'),
               [Input('viewtype','value')])
def updateColumns(viewtype):
    if viewtype == 'days':
        cols =['Days',
               'Deposits',
               'Insurance',
               'Plugs',
               'Packs Payments',
               'Total',
               'Variance']
        return cols
    else:
        cols =['Month',
               'Deposits',
               'Insurance',
               'Plugs',
               'Packs Payments',
               'Total',
               'Variance']
        return cols

@app.callback(Output('bank_table','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date'),
               Input('viewtype','value')])
def updateBankTable(start_date,end_date,viewtype):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()

        if viewtype == 'days':
            bankDF = buildBankTable(df1,start_date,end_date,viewtype)
            bankDF['Days'] = bankDF['Days'].astype(str)
            bankDF = bankDF.append(bankDF.sum(numeric_only=True),ignore_index=True)
            bankDF = bankDF.round(2)
            bankDF['Days'].fillna('Total',inplace=True)
        else:
            bankDF = buildBankTable(df1,start_date,end_date,viewtype)
            bankDF['Month'] = bankDF['Month'].astype(str)
            bankDF = bankDF.append(bankDF.sum(numeric_only=True),ignore_index=True)
            bankDF = bankDF.round(2)
            bankDF['Month'].fillna('Total',inplace=True)

        return bankDF.to_dict('records',into=OrderedDict)

@app.callback(Output('sunpath_table','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date'),
               Input('viewtype','value')])
def updateSunpathTable(start_date,end_date,viewtype):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()

        if viewtype == 'days':
            sunpathDF = buildTableSunpath(start_date,end_date,viewtype)
            sunpathDF['Days'] = sunpathDF['Days'].astype(str)
            sunpathDF = sunpathDF.append(sunpathDF.sum(numeric_only=True),ignore_index=True)
            sunpathDF = sunpathDF.round(2)
            sunpathDF['Days'].fillna('Total',inplace=True)
        else:
            sunpathDF = buildTableSunpath(start_date,end_date,viewtype)
            sunpathDF['Month'] = sunpathDF['Month'].astype(str)
            sunpathDF = sunpathDF.append(sunpathDF.sum(numeric_only=True),ignore_index=True)
            sunpathDF = sunpathDF.round(2)
            sunpathDF['Month'].fillna('Total',inplace=True)
        return sunpathDF.to_dict('records',into=OrderedDict)
