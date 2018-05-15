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

import dash
from dash.dependencies import Input, Output, State
import dash_html_components as html
import dash_core_components as dcc
import dash_table_experiments as dte
from app import app

#Get initial Dataframe to work with
df1 = pd.read_pickle('./static/data/Banking_Transaction.pkl')
df2 = pd.read_pickle('./static/data/Plug_Other.pkl')
df3 = pd.read_pickle('./static/data/Deposits.pkl')
df4 = pd.read_pickle('./static/data/Payments.pkl')
df5 = pd.read_pickle('./static/data/SPF_Premium.pkl')

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
                html.H1('Bank Reconcillation Admin Detail Summary Dashboard',
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
                            id='summary',
                            rows=[{}],
                            columns=['ClearDate',
                                     'BnkAmt',
                                     'DepAmt',
                                     'PaymntsAmt',
                                     'PlugAmt',
                                     'InsRsvAmt',
                                     'Variance'],
                            sortable=True,
                            editable=False
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
                            id='bank_transaction',
                            rows=[{}],
                            columns=['ClearDate',
                                     'Description',
                                     'Notes',
                                     'BnkAmt'],
                            #column_widths=[100,750],
                            sortable=True,
                            editable=False,
                            max_rows_in_viewport=5
                    ),
                ],
                className='eleven columns'
            ),
        ],
        className='row'
    ),

    html.Div(
        [
            html.Div(
                [
                    html.H4('Plug Other'),
                    dte.DataTable(
                            id='plug_other',
                            rows=[{}],
                            columns=['ClearDate',
                                     'Description',
                                     'Notes',
                                     'Account',
                                     'PlugAmt'],
                            #column_widths=[100,750],
                            sortable=True,
                            editable=False,
                            max_rows_in_viewport=5
                    ),
                ],
                className='eleven columns'
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
                            id='deposits',
                            rows=[{}],
                            columns=['ClearDate',
                                     'Payee',
                                     'GroupName',
                                     'DepAmt'],
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
                            id='payments',
                            rows=[{}],
                            columns=['ClearDate',
                                     'Vendor',
                                     'GroupName',
                                     'PaymntsAmt'],
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
                    html.H4('Insurance'),
                    dte.DataTable(
                            id='insurance',
                            rows=[{}],
                            columns=['ClearDate',
                                     'InsRsvAmt'],
                            sortable=True,
                            editable=False,
                            max_rows_in_viewport=5
                    ),
                ],
                className='three columns'
            ),
        ],
        className='row'
    ),
])
@cache.memoize()
def getAggregDates(df,start_date,end_date):
    return df.loc[(df['ClearDate']>=start_date)
              & (df['ClearDate']<=end_date)].groupby('ClearDate').sum()

@cache.memoize()
def getDataframe(df,start_date,end_date):
    df = df.loc[(df['ClearDate']>=start_date) & (df['ClearDate']<=end_date)]
    df = df.append(df.sum(numeric_only=True),ignore_index=True)
    df = df.round(2)
    df['ClearDate'].fillna('Total',inplace=True)
    return df

@cache.memoize()
def buildSummary(start_date,end_date):
    DF1 = getAggregDates(df1,start_date,end_date)
    DF2 = getAggregDates(df2,start_date,end_date)
    DF3 = getAggregDates(df3,start_date,end_date)
    DF4 = getAggregDates(df4,start_date,end_date)
    DF5 = getAggregDates(df5,start_date,end_date)

    DF = DF1.merge(DF3,how='outer',left_index=True,right_index=True)
    DF = DF.merge(DF4,how='outer',left_index=True,right_index=True)
    DF = DF.merge(DF2,how='outer',left_index=True,right_index=True)
    DF = DF.merge(DF5,how='outer',left_index=True,right_index=True)
    DF.reset_index(level=0,inplace=True)
    DF['ClearDate'] = pd.to_datetime(DF['ClearDate'],format="%Y-%m-%d")
    DF['ClearDate'] = DF['ClearDate'].apply(lambda x: x.date())
    DF['Variance'] = DF[['DepAmt','PaymntsAmt','PlugAmt','InsRsvAmt']].sum(axis=1,skipna=True).round(2)
    DF['Variance'] = DF['Variance']-DF['BnkAmt']
    DF = DF.append(DF.sum(numeric_only=True),ignore_index=True)
    DF['ClearDate'].fillna('Total',inplace=True)
    return DF.round(2)




@app.callback(Output('summary','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updateSummary(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        dataframe = buildSummary(start_date,end_date)
        return dataframe.to_dict('records',into=OrderedDict)

@app.callback(Output('bank_transaction','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updateBankTransaction(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = getDataframe(df1,start_date,end_date)
        return df.to_dict('records',into=OrderedDict)

@app.callback(Output('plug_other','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updatePlug(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = getDataframe(df2,start_date,end_date)
        return df.to_dict('records',into=OrderedDict)

@app.callback(Output('deposits','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updateDeposit(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = getDataframe(df3,start_date,end_date)
        return df.to_dict('records',into=OrderedDict)

@app.callback(Output('payments','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updatePayment(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = getDataframe(df4,start_date,end_date)
        return df.to_dict('records',into=OrderedDict)

@app.callback(Output('insurance','rows'),
               [Input('date_range','start_date'),
               Input('date_range','end_date')])
def updateSPFPrem(start_date,end_date):
    if (start_date is not None) and (end_date is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = df5.loc[(df5['ClearDate']>=start_date) & (df5['ClearDate']<=end_date)]
        df = df.round(2)
        return df.to_dict('records',into=OrderedDict)

#Main
#if __name__ == '__main__':
#    app.run_server(debug=True)
