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

from controls import SHORTCODES

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
                html.H1('Bank Recon Funder Detail Summary Dashboard',
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
                            end_date=dt.now().date(),
                            start_date=dt(2018,1,1).date(),
                            minimum_nights=0,
                    ),
            ],
            className='five columns'
        ),
    ],className='row'),

    #Summary Table
    html.Div(
        [
            html.Div(
                [
                    html.H4('Summary'),
                    dte.DataTable(
                            id='summary_6',
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
                            sortable=True,
                            editable=False,
                            max_rows_in_viewport=10
                    ),
                ],
                className='ten columns'
            ),
        ],
        className='row'
    ),

    #First Row Table
    html.Div(
        [
            html.Div(
                [
                    html.H4('Bank Transactions'),
                    dte.DataTable(
                            id='bank_transaction_6',
                            rows=[{}],
                            columns=['Date',
                                     'Description',
                                     'Amount'],
                            #column_widths=[100,750,200],
                            sortable=True,
                            editable=False,
                            max_rows_in_viewport=5
                    ),
                ],
                className='six columns'
            ),

            html.Div(
                [
                    html.H4('Plugs'),
                    dte.DataTable(
                            id='plug_6',
                            rows=[{}],
                            columns=['Date',
                                     'Description',
                                     'Amount'],
                            #column_widths=[100,750],
                            sortable=True,
                            editable=False,
                            max_rows_in_viewport=5
                    ),
                ],
                className='six columns'
            ),
        ],
        className='row'
    ),

    #Second Table
    html.Div(
        [
            html.Div(
                [
                    html.H4('Deposits'),
                    dte.DataTable(
                            id='deposits_6',
                            rows=[{}],
                            columns=['Date',
                                     'Description',
                                     'Vendor',
                                     'Amount'],
                            #column_widths=[100,300,170],
                            sortable=True,
                            editable=False,
                            max_rows_in_viewport=5
                    ),
                ],
                className='six columns'
            ),

            html.Div(
                [
                    html.H4('Payments'),
                    dte.DataTable(
                            id='payments_6',
                            rows=[{}],
                            columns=['Date',
                                     'Vendor',
                                     'Amount'],
                            #column_widths=[100,300,170],
                            sortable=True,
                            editable=False,
                            max_rows_in_viewport=5
                    ),
                ],
                className='six columns'
            ),
        ],
        className='row'
    ),

    html.Div(
        [
            html.Div(
                [
                    html.H4('Customer Collections'),
                    dte.DataTable(
                            id='cust_collections_6',
                            rows=[{}],
                            columns=['Date',
                                     'Description',
                                     'Amount'],
                            #column_widths=[100,300,170],
                            sortable=True,
                            editable=False,
                            max_rows_in_viewport=5
                    ),
                ],
                className='six columns'
            ),

            html.Div(
                [
                    html.H4('Insurance'),
                    dte.DataTable(
                            id='insurance_6',
                            rows=[{}],
                            columns=['Date',
                                     'Description',
                                     'Amount'],
                            sortable=True,
                            editable=False
                    ),
                ],
                className='five columns'
            ),
        ],
        className='row'
    ),

    html.Div(
        [
            html.Div(
                [
                    html.H4('Collections'),
                    dte.DataTable(
                            id='collections_6',
                            rows=[{}],
                            columns=['Date',
                                     'Vendor',
                                     'Amount'],
                            #column_widths=[100,300,170],
                            sortable=True,
                            editable=False,
                            max_rows_in_viewport=5
                    ),
                ],
                className='six columns'
            ),

        ],
        className='row'
    ),
])

@cache.memoize()
def buildDayRange(start_date,end_date):
    return range(start_date.day,end_date.day+1)

@cache.memoize()
def buildBankAmt(dataframe,start_date,end_date):
    df = dataframe.copy()
    df = df.loc[(df['ClearDate']>=start_date) & (df['ClearDate']<=end_date)]
    dates = sorted(list(set(df.ClearDate)))
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

@cache.memoize()
def buildBankRecon(dataframe,dataframe2,start_date,end_date):
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


@cache.memoize()
def getTable(dataframe,col_name,start_date,end_date):
    if col_name == 'Bank' or col_name == 'Plug':
        df = dataframe.loc[(dataframe['ClearDate']>=start_date)
                           & (dataframe['ClearDate']<=end_date)]
    else:
        df = dataframe.loc[(dataframe['TxDate']>=start_date)
                          & (dataframe['TxDate']<=end_date)]

    if col_name == 'Bank':
        cols = ['ClearDate','Description','Amount']
        df = df[cols]
        df = df.append(df.sum(numeric_only=True),ignore_index=True)
        df = df.round(2)
        df['ClearDate'].fillna('Total',inplace=True)
        df.rename(index=str,columns={'ClearDate': 'Date'},inplace=True)
        return df
    elif col_name == 'Plug':
        cols = ['ClearDate','Description','Amount']
        df = df[cols].loc[df['Category']==col_name]
        df = df.append(df.sum(numeric_only=True),ignore_index=True)
        df = df.round(2)
        df['ClearDate'].fillna('Total',inplace=True)
        df.rename(index=str,columns={'ClearDate': 'Date'},inplace=True)
        return df
    elif col_name == 'Deposits':
        cols = ['TxDate','TxDescription','Vendor','Amount']
        df = df.loc[df.Category==col_name]
        df['Vendor'] = df['PaidFrom'].apply(lambda x: SHORTCODES[x])
        df = df[cols].groupby(['TxDate','TxDescription','Vendor']).sum().reset_index()
        df = df.append(df.sum(numeric_only=True),ignore_index=True)
        df = df.round(2)
        df['TxDate'].fillna('Total',inplace=True)
        df.rename(index=str,columns={'TxDate':'Date','TxDescription':'Description'},inplace=True)
        return df
    elif col_name == 'Payments':
        cols = ['TxDate','TxDescription','Vendor','Amount']
        df = df.loc[df.Category==col_name]
        df['Vendor'] = df['PaidTo'].apply(lambda x: SHORTCODES[x])
        df = df[cols].groupby(['TxDate','TxDescription','Vendor']).sum().reset_index()
        df = df.append(df.sum(numeric_only=True),ignore_index=True)
        df = df.round(2)
        df['TxDate'].fillna('Total',inplace=True)
        df.rename(index=str,columns={'TxDate':'Date','TxDescription':'Description'},inplace=True)
        return df
    elif col_name == 'Customer Collections':
        cols = ['TxDate','TxDescription','Amount']
        df = df.loc[df.Category==col_name]
        df = df[cols].groupby(['TxDate','TxDescription']).sum().reset_index()
        df = df.append(df.sum(numeric_only=True),ignore_index=True)
        df = df.round(2)
        df['TxDate'].fillna('Total',inplace=True)
        df.rename(index=str,columns={'TxDate':'Date','TxDescription':'Description'},inplace=True)
        return df
    elif col_name == 'Collections':
        cols = ['TxDate','Vendor','Amount']
        df = df.loc[df.Category==col_name]
        df['Vendor'] = df['PaidFrom'].apply(lambda x: SHORTCODES[x])
        df = df[cols].groupby(['TxDate','Vendor']).sum().reset_index()
        df = df.append(df.sum(numeric_only=True),ignore_index=True)
        df = df.round(2)
        df['TxDate'].fillna('Total',inplace=True)
        df.rename(index=str,columns={'TxDate':'Date'},inplace=True)
        return df
    elif col_name == 'Insurance':
        cols = ['TxDate','TxDescription','Amount']
        df = df.loc[df.Category==col_name]
        df = df[cols].groupby(['TxDate','TxDescription']).sum().reset_index()
        df = df.append(df.sum(numeric_only=True),ignore_index=True)
        df = df.round(2)
        df['TxDate'].fillna('Total',inplace=True)
        df.rename(index=str,columns={'TxDate':'Date','TxDescription':'Description'},inplace=True)
        return df

@app.callback(Output('summary_6','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updateSummary(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df1 = buildBankAmt(DF1,start_date,end_date)
        df2 = buildBankRecon(DF1,DF2,start_date,end_date)
        bankDF = df1.merge(df2,on='Day')
        bankDF['Variance'] = bankDF['BankAmtTotal']-bankDF['Total']
        bankDF['Day'] = bankDF['Day'].astype(str)
        bankDF = bankDF.append(bankDF.sum(numeric_only=True),ignore_index=True)
        bankDF = bankDF.round(2)
        bankDF['Day'].fillna('Total',inplace=True)
        return bankDF.to_dict('records',into=OrderedDict)

@app.callback(Output('bank_transaction_6','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updateBankTransaction(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = getTable(DF1,'Bank',start_date,end_date)
        return df.to_dict('records',into=OrderedDict)

@app.callback(Output('plug_6','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updatePlug(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = getTable(DF1,'Plug',start_date,end_date)
        return df.to_dict('records',into=OrderedDict)

@app.callback(Output('deposits_6','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updateDeposit(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = getTable(DF2,'Deposits',start_date,end_date)
        return df.to_dict('records',into=OrderedDict)

@app.callback(Output('payments_6','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updatePayments(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = getTable(DF2,'Payments',start_date,end_date)
        return df.to_dict('records',into=OrderedDict)

@app.callback(Output('cust_collections_6','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updateCustomerCollections(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = getTable(DF2,'Customer Collections',start_date,end_date)
        return df.to_dict('records',into=OrderedDict)

@app.callback(Output('insurance_6','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updateInsurance(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = getTable(DF2,'Insurance',start_date,end_date)
        return df.to_dict('records',into=OrderedDict)

@app.callback(Output('collections_6','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updateCollections(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = getTable(DF2,'Collections',start_date,end_date)
        return df.to_dict('records',into=OrderedDict)
#Main
#if __name__ == '__main__':
#    app.run_server(debug=True)
