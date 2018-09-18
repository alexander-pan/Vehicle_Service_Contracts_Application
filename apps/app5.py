#import required libraries
from datetime import datetime as dt
from dateutil.relativedelta import *
import pandas as pd
import numpy as np
from flask import Flask
from flask_caching import Cache
from collections import OrderedDict
import pyodbc
import copy
from app import app

import dash
from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import dash_table_experiments as dte

#Server credential Setup

#Get initial Dataframe to work with
DF1 = pd.read_pickle('./static/data/SPA_FundingBankStat.pkl')
DF2 = pd.read_pickle('./static/data/TransactionLog.pkl')

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
                html.H1('Bank Reconcillation Funder Dashboard',
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
                    html.Div([
                        dte.DataTable(
                                id='bank_table_total',
                                rows=[{}],
                                columns=['Day','BankAmtTotal'],
                                #column_widths=[50],
                                sortable=True,
                                editable=False,
                                max_rows_in_viewport=32
                        ),
                    ],className='three columns'),

                    html.Div([
                        dte.DataTable(
                                id='bank_table_types',
                                rows=[{}],
                                columns=['Day',
                                         'DepositsAmt',
                                         'InsuranceAmt',
                                         'PaymentAmt',
                                         'PlugAmt',
                                         'CustCollAmt',
                                         'CollectAmt',
                                         'Total',
                                         'Variance'],
                                #column_widths=[50],
                                sortable=True,
                                editable=False,
                                max_rows_in_viewport=32
                        ),
                    ],className='nine columns',style={'margin-left':50}),
                ],
            ),
        ],
        className='row',style={'margin-top':20}
    ),
])

"""@cache.memoize()
def buildDays(date):
    month = date.month
    thirtyone = [1,3,5,7,8,10,12]
    thirty = [4,9,11]

    if month in thirtyone:
        days = range(1,32)
    elif month in thirty:
        days = range(1,31)
    elif date.year%4 == 0:
        days = range(1,30)
    else:
        days = range(1,29)

    return days"""

@cache.memoize()
def buildDayRange(start_date,end_date):
    delta = end_date - start_date
    return range(start_date.day,delta.days+1)
    #return range(start_date.day,end_date.day+1)

@cache.memoize()
def convertDatetime(df,col):
    df[col] = pd.to_datetime(df[col],format="%m/%d/%y")
    df[col] = df[col].apply(lambda x: x.date())
    return df[col]

@cache.memoize()
def buildBankAmt(dataframe,start_date,end_date,viewtype):
    df = dataframe.copy()
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
                total_amt = temp.Amount.sum()
                table.append((day,total_amt))
            else:
                table.append((day,np.nan))
        result_df = pd.DataFrame(table,columns=['Day','BankAmtTotal'])
        result_df.fillna(0,inplace=True)
        return result_df.round(2)
    else:
        df['Date'] = df['ClearDate'].apply(lambda x: x.strftime('%B'))
        monthDates = set([x.strftime('%B') for x in dates])
        table = []
        for month in monthDates:
            temp = df.loc[df['Date']==month]
            total_amt = temp.Amount.sum()
            table.append((month,total_amt))
        result_df = pd.DataFrame(table,columns=['Month','BankAmtTotal'])
        result_df.fillna(0,inplace=True)
        return result_df.round(2)

@cache.memoize()
def buildBankRecon(dataframe,dataframe2,start_date,end_date,viewtype):
    df1 = dataframe.copy()
    df2 = dataframe2.copy()
    accounts = df1.Category.unique().tolist()
    accounts.append('Customer Collections')
    accounts.append('Collections')
    df1 = df1.loc[(df1['ClearDate']>=start_date)
                  & (df1['ClearDate']<=end_date)]
    df2 = df2.loc[(df2['TxDate']>=start_date)
                  & (df2['TxDate']<=end_date)]
    dates = sorted(list(set(df1.ClearDate)))
    if viewtype == 'days':
        dayDates = [x.day for x in dates]
        dictDates = dict([(x.day,x) for x in dates])

        #build days
        days = buildDayRange(start_date,end_date)
        table = []
        for day in days:
            if day in dayDates:
                acc_amts = []
                for acc in accounts:
                    date = dictDates[day]
                    if acc == 'Plug':
                        temp = df1.loc[df1['ClearDate']==date]
                    else:
                        temp = df2.loc[df2['TxDate']==date]
                    df_acc = temp.loc[temp['Category']==acc]
                    amt = df_acc.Amount.sum()
                    acc_amts.append(amt)
                table.append((day,
                              acc_amts[0],
                              acc_amts[1],
                              acc_amts[2],
                              acc_amts[3],
                              acc_amts[4],
                              acc_amts[5],
                              sum(acc_amts)))
            else:
                table.append((day,np.nan))
        accounts.insert(0,'Day')
    else:
        df1['Date'] = df1['ClearDate'].apply(lambda x: x.strftime('%B'))
        df2['Date'] = df2['TxDate'].apply(lambda x: x.strftime('%B'))
        monthDates = set([x.strftime('%B') for x in dates])
        table = []
        for month in monthDates:
            acc_amts = []
            for acc in accounts:
                if acc == 'Plug':
                    temp = df1.loc[df1['Date']==month]
                else:
                    temp = df2.loc[df2['Date']==month]
                df_acc = temp.loc[temp['Category']==acc]
                amt = df_acc.Amount.sum()
                acc_amts.append(amt)
            table.append((month,
                          acc_amts[0],
                          acc_amts[1],
                          acc_amts[2],
                          acc_amts[3],
                          acc_amts[4],
                          acc_amts[5],
                          sum(acc_amts)))
        accounts.insert(0,'Month')
    accounts.append('Total')
    result_df = pd.DataFrame(table,columns=accounts)
    result_df.rename(index=str,columns={'Plug':'PlugAmt',
                      'Deposits':'DepositsAmt',
                      'Payments':'PaymentAmt',
                      'Insurance':'InsuranceAmt',
                      'Customer Collections': 'CustCollAmt',
                      'Collections': 'CollectAmt'},inplace=True)
    result_df.fillna(0,inplace=True)
    return result_df.round(2)

@app.callback(Output('bank_table_total','columns'),
              [Input('viewtype','value')])
def updateRows1(viewtype):
    if viewtype == 'days':
        columns=['Day',
                 'BankAmtTotal']
        return columns
    else:
        columns=['Month',
                 'BankAmtTotal']
        return columns

@app.callback(Output('bank_table_types','columns'),
              [Input('viewtype','value')])
def updateRows2(viewtype):
    if viewtype == 'days':
        columns=['Day',
                 'DepositsAmt',
                 'InsuranceAmt',
                 'PaymentAmt',
                 'PlugAmt',
                 'CustCollAmt',
                 'CollectAmt',
                 'Total',
                 'Variance']
        return columns
    else:
        columns=['Month',
                 'DepositsAmt',
                 'InsuranceAmt',
                 'PaymentAmt',
                 'PlugAmt',
                 'CustCollAmt',
                 'CollectAmt',
                 'Total',
                 'Variance']
        return columns



@app.callback(Output('bank_table_total','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date'),
               Input('viewtype','value')])
def updateBankTable1(start_date,end_date,viewtype):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        if viewtype == 'days':
            df1 = buildBankAmt(DF1,start_date,end_date,viewtype)
            df1['Day'] = df1['Day'].astype(str)
            df1 = df1.append(df1.sum(numeric_only=True),ignore_index=True)
            df1 = df1.round(2)
            df1['Day'].fillna('Total',inplace=True)
        else:
            df1 = buildBankAmt(DF1,start_date,end_date,viewtype)
            df1['Month'] = df1['Month'].astype(str)
            df1 = df1.append(df1.sum(numeric_only=True),ignore_index=True)
            df1 = df1.round(2)
            df1['Month'].fillna('Total',inplace=True)
        return df1.to_dict('records',into=OrderedDict)

@app.callback(Output('bank_table_types','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date'),
               Input('viewtype','value')])
def updateBankTable2(start_date,end_date,viewtype):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()

        if viewtype == 'days':
            df1 = buildBankAmt(DF1,start_date,end_date,viewtype)
            df2 = buildBankRecon(DF1,DF2,start_date,end_date,viewtype)
            bankDF = df1.merge(df2,on='Day')
            bankDF['Variance'] = bankDF['BankAmtTotal']-bankDF['Total']
            bankDF['Day'] = bankDF['Day'].astype(str)
            bankDF = bankDF.append(bankDF.sum(numeric_only=True),ignore_index=True)
            bankDF = bankDF.round(2)
            bankDF['Day'].fillna('Total',inplace=True)
        else:
            df1 = buildBankAmt(DF1,start_date,end_date,viewtype)
            df2 = buildBankRecon(DF1,DF2,start_date,end_date,viewtype)
            bankDF = df1.merge(df2,on='Month')
            bankDF['Variance'] = bankDF['BankAmtTotal']-bankDF['Total']
            bankDF['Month'] = bankDF['Month'].astype(str)
            bankDF = bankDF.append(bankDF.sum(numeric_only=True),ignore_index=True)
            bankDF = bankDF.round(2)
            bankDF['Month'].fillna('Total',inplace=True)
        return bankDF.to_dict('records',into=OrderedDict)
