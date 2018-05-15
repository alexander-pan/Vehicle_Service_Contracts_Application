#import required libraries
from datetime import datetime as dt
from dateutil.relativedelta import *
import pandas as pd
import numpy as np
from collections import OrderedDict
import copy
import plotly.graph_objs as go
from dash.dependencies import Input, Output
import dash_html_components as html
import dash_core_components as dcc
from flask_caching import Cache
from app import app

#import dash
#from flask import Flask
#import pyodbc
#import os

#Multi-Dropdown options
from controls import SELLERS, FUNDERS


#Get initial Dataframe to work with
"""q1 = "select * from dbo.daily_extract;"
q2 = "select * from dbo.seller_funding_data"
df1 = pd.read_sql(q1,cnxn)
df2 = pd.read_sql(q2,cnxn)"""
df1 = pd.read_pickle('./data/daily_extract.pkl')
df2 = pd.read_pickle('./data/seller_funding_data.pkl')
df3 = pd.read_pickle('./data/admin_funding_data.pkl')

#Setup App
#server = Flask(__name__)
#server.secret_key = os.environ.get('secret_key', 'secret')

#app = dash.Dash(__name__,server=server)
app.css.append_css({"external_url":"static/dashboard.css"})

#app.config.suppress_callback_exceptions = True
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

#Application Layout
layout_page = html.Div([
    dcc.Link('Home Page',href='/'),
    html.Div([
        html.Div(
            [
                html.H1('Vender Current Snapshot Dashboard',
                        style={'textAlign': 'center'}),
            ],
            className='row'
        ),

        html.Div(
            [
                html.Label('Funder'),
                    dcc.Dropdown(
                        id = 'funder',
                        options=funder_options,
                        placeholder='Select a Funder',
                    ),
            ],
            className='three columns'
        ),

        html.Div(
            [
                html.Label('Seller'),
                    dcc.Dropdown(
                        id = 'seller',
                        options=seller_options,
                        placeholder='Select a Seller'
                    ),
            ],
            className='four columns'
        ),

        html.Div(
            [
                html.Label('Date Range for Effective Date'),
                    dcc.DatePickerRange(
                            id = 'date_range',
                            number_of_months_shown=3,
                            end_date=dt.now().date(),
                            start_date=(dt.now()+relativedelta(years=-2)).date()
                            #start_date_placeholder_text="Start Date"
                    ),
            ],
            className='four columns'
        ),
    ],className='row'),

    #First Row of Values
    html.Div(
        [
            html.Div(
                [
                    dcc.Textarea(
                        id='VSC_funded',
                        value='$0',
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
                    html.Label('VSC Amount Financed',
                        style={'fontSize':20,
                               'textAlign':'center'}),
                ],
                className='five columns'
            ),

            html.Div(
                [
                    dcc.Textarea(
                        id='seller_advance',
                        value='$0',
                        readOnly=True,
                        style={'textAlign': 'center',
                               'align': 'center',
                               'fontSize':50,
                               'color': '#CCCCCC',
                               'backgroundColor': '#191A1A',
                               'width': '100%',
                               'margin':10
                              }
                    ),
                    html.Label('Seller Advance',
                        style={'fontSize':20,
                               'textAlign':'center'}),
                ],
                className='five columns'
            ),
        ],
        className='row'
    ),

    #Second Row of Values
    html.Div(
        [
            html.Div(
                [
                    dcc.Textarea(
                        id='seller_reserve',
                        value='$0',
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
                    html.Label('Seller Reserve',
                        style={'fontSize':20,
                               'textAlign':'center'}),
                ],
                className='five columns'
            ),

            html.Div(
                [
                    dcc.Textarea(
                        id='seller_cost',
                        value='$0',
                        readOnly=True,
                        style={'textAlign': 'center',
                               'align': 'center',
                               'fontSize':50,
                               'color': '#CCCCCC',
                               'backgroundColor': '#191A1A',
                               'width': '100%',
                               'margin':10
                              }
                    ),
                    html.Label('Seller Cost',
                        style={'fontSize':20,
                               'textAlign':'center'}),
                ],
                className='five columns'
            ),
        ],
        className='row'
    ),

    #Second Row of Values
    html.Div(
        [
            html.Div(
                [
                    dcc.Textarea(
                        id='funding_fee',
                        value='$0',
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
                    html.Label('Funding Fee',
                        style={'fontSize':20,
                               'textAlign':'center'}),
                ],
                className='five columns'
            ),
        ],
        className='row'
    ),

    #Plots
    html.Div(
        [
            html.Div(
                [
                    dcc.Checklist(
                        id = 'curve_list',
                        options = [
                            {'label': 'Cohort 1(1-12)','value': 1},
                            {'label': 'Cohort 2(13-15)','value': 2},
                            {'label': 'Cohort 3(16-18)','value': 3},
                            {'label': 'Cohort 4(19-24)','value':4},
                            {'label': 'All (1-24)','value':5}
                        ],
                        values=[5],
                        labelStyle={'display':'inline-block'}
                    ),
                    dcc.Graph(id='cancellation_curve')
                ],
                #className='six columns',
                style={'margin-top':'20'}
            ),
        ],
        className='row'
    ),
    #Plots
    html.Div(
        [
            html.Div(
                [
                    dcc.Checklist(
                        id = 'msv_list',
                        options = [
                            {'label': 'Cohort 1(1-12)','value': 1},
                            {'label': 'Cohort 2(13-15)','value': 2},
                            {'label': 'Cohort 3(16-18)','value': 3},
                            {'label': 'Cohort 4(19-24)','value':4},
                            {'label': 'All (1-24)','value':5}
                        ],
                        values=[5],
                        labelStyle={'display':'inline-block'}
                    ),
                    dcc.Graph(id='monthly_sales')
                ],
                #className='six columns',
                style={'margin-top':'20'}
            ),
        ],
        className='row'
    ),

])

@cache.memoize()
def dataframe(start_date,end_date,funder,seller,df1,df2):
    tempDF1 = df1.loc[(df1['EffectiveDate'] >= start_date)
                & (df1['EffectiveDate'] < end_date)]
    tempDF2 = df2.loc[(df2['SellerName']==seller ) & (df2['FundCo']==funder)]
    df = tempDF1.merge(tempDF2,how='inner',left_on='AccountNumber',right_on='AccountNumber')
    return df

@cache.memoize()
def cancel_curve(DF,cohort):
    df = DF.copy()

    #identify cohorts
    if cohort == 1:
        df = DF.loc[(DF.Installments > 0) & (DF.Installments < 13)]
    elif cohort == 2:
        df = DF.loc[(DF.Installments > 12) & (DF.Installments < 16)]
    elif cohort == 3:
        df = DF.loc[(DF.Installments > 15) & (DF.Installments < 19)]
    elif cohort == 4:
        df = DF.loc[(DF.Installments > 18) & (DF.Installments < 25)]
    elif cohort == 5:
        pass

    #Find current cancellation curves percentages
    tupes = zip(range(0,25),range(4,29))
    payments,cancel_percent = [],[]

    for i,j in tupes:
        qfc_date = (dt.now()+relativedelta(months=-j)).date()
        num = float(df.loc[(df['IsCancelled']==1)
                           & (df['PaymentsMade_x']==i)
                           & (df['EffectiveDate_x'] < qfc_date)].shape[0])
        den = float(df.loc[(df['EffectiveDate_x'] < qfc_date)].shape[0])

        if den != 0:
            payments.append(i+1)
            cancel_percent.append(num/den*100.0)

    return payments,cancel_percent

@cache.memoize()
def monthly_sales(DF,cohort):
    df = DF.copy()

    #identify cohorts
    if cohort == 1:
        df = DF.loc[(DF.Installments > 0) & (DF.Installments < 13)]
    elif cohort == 2:
        df = DF.loc[(DF.Installments > 12) & (DF.Installments < 16)]
    elif cohort == 3:
        df = DF.loc[(DF.Installments > 15) & (DF.Installments < 19)]
    elif cohort == 4:
        df = DF.loc[(DF.Installments > 18) & (DF.Installments < 25)]
    elif cohort == 5:
        pass

    months = OrderedDict()
    for date in sorted(df['EffectiveDate_x']):
        key = date.strftime('%b %y')
        if key in months:
            months[key] += 1
        else:
            months[key] = 1

    label = [x[0] for x in months.iteritems()]
    count = [x[1] for x in months.iteritems()]
    return label,count

@app.callback(Output('date_range','initial_visible_month'),
               [Input('date_range','start_date')])
def update_IntVisMonth(start_date):
    if (start_date is None):
        #Get minimum dates
        min_month = (dt.now()+relativedelta(months=-28)).month
        min_day = (dt.now()+relativedelta(months=-28)).day
        min_year = (dt.now()+relativedelta(months=-28)).year
        return dt(min_year,min_month,min_day)
    elif (start_date is not None):
        return dt.strptime(start_date,'%Y-%m-%d')


@app.callback(Output('VSC_funded','value'),
               [Input('date_range','start_date'),
               Input('date_range','end_date'),
               Input('funder','value'),
               Input('seller','value')])
def update_VSCFunded(start_date,end_date,funder,seller):
    print type(start_date)
    if (start_date is not None) and (end_date is not None) and (funder is not None) and (seller is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = dataframe(start_date,end_date,funder,seller,df1,df2)
        amount = '${:,.2f}'.format(round(df['AmountFinanced'].sum(),2))
        return '%s' % amount

@app.callback(Output('seller_advance','value'),
               [Input('date_range','start_date'),
               Input('date_range','end_date'),
               Input('funder','value'),
               Input('seller','value')])
def update_SellerAdvance(start_date,end_date,funder,seller):
    if (start_date is not None) and (end_date is not None) and (funder is not None) and (seller is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = dataframe(start_date,end_date,funder,seller,df1,df2)
        amount = '${:,.2f}'.format(round(df['SellerAdvanceAmount'].sum(),2))
        return '%s' % amount

@app.callback(Output('seller_reserve','value'),
               [Input('date_range','start_date'),
               Input('date_range','end_date'),
               Input('funder','value'),
               Input('seller','value')])
def update_SellerReserve(start_date,end_date,funder,seller):
    if (start_date is not None) and (end_date is not None) and (funder is not None) and (seller is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = dataframe(start_date,end_date,funder,seller,df1,df2)
        amount = '${:,.2f}'.format(round(df['CancelReserveAmount'].sum(),2))
        return '%s' % amount

@app.callback(Output('seller_cost','value'),
               [Input('date_range','start_date'),
               Input('date_range','end_date'),
               Input('funder','value'),
               Input('seller','value')])
def update_SellerCost(start_date,end_date,funder,seller):
    if (start_date is not None) and (end_date is not None) and (funder is not None) and (seller is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = dataframe(start_date,end_date,funder,seller,df1,df2)
        amount = '${:,.2f}'.format(round(df['SellerCost'].sum(),2))
        return '%s' % amount

@app.callback(Output('funding_fee','value'),
               [Input('date_range','start_date'),
               Input('date_range','end_date'),
               Input('funder','value'),
               Input('seller','value')])
def update_FundingFee(start_date,end_date,funder,seller):
    if (start_date is not None) and (end_date is not None) and (funder is not None) and (seller is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = dataframe(start_date,end_date,funder,seller,df1,df2)
        amount = '${:,.2f}'.format(round(df['DiscountAmount_x'].sum(),2))
        return '%s' % amount

@app.callback(Output('cancellation_curve','figure'),
              [Input('date_range','start_date'),
              Input('date_range','end_date'),
              Input('funder','value'),
              Input('seller','value'),
              Input('curve_list','values')])
def update_cancel_curve(start_date,end_date,funder,seller,curve_list):
    layout_curve = copy.deepcopy(layout)

    if (start_date is not None) and (end_date is not None) and (funder is not None) and (seller is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = dataframe(start_date,end_date,funder,seller,df1,df2)

        datas = []
        for val in curve_list:
            if val == 1:
                x_data,y_data = cancel_curve(df,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        name = 'Cohort 1'
                    )
                datas.append(data)
            elif val == 2:
                x_data,y_data = cancel_curve(df,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        name = 'Cohort 2'
                    )
                datas.append(data)
            elif val == 3:
                x_data,y_data = cancel_curve(df,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        name = 'Cohort 3'
                    )
                datas.append(data)
            elif val == 4:
                x_data,y_data = cancel_curve(df,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        name = 'Cohort 4'
                    )
                datas.append(data)
            elif val == 5:
                x_data,y_data = cancel_curve(df,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        name = 'All'
                    )
                datas.append(data)
        layout_curve['title'] = 'Cancellation Rates Per Payments Made'
        layout_curve['xaxis'] = {'title': 'Payments Made'}
        layout_curve['yaxis'] = {'title': 'Cancellation % Rate '}
        layout_curve['barmode'] = 'group'
        fig = go.Figure(data=datas,layout=layout_curve)
        return fig

@app.callback(Output('monthly_sales','figure'),
              [Input('date_range','start_date'),
              Input('date_range','end_date'),
              Input('funder','value'),
              Input('seller','value'),
              Input('msv_list','values')])
def update_monthly_sales(start_date,end_date,funder,seller,msv_list):
    layout_sales = copy.deepcopy(layout)

    if (start_date is not None) and (end_date is not None) and (funder is not None) and (seller is not None):
        start_date = dt.strptime(start_date,'%Y-%m-%d').date()
        end_date = dt.strptime(end_date,'%Y-%m-%d').date()
        df = dataframe(start_date,end_date,funder,seller,df1,df2)
        datas = []
        for val in msv_list:
            if val == 1:
                x_data,y_data = monthly_sales(df,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        name = 'Cohort 1'
                    )
                datas.append(data)
            elif val == 2:
                x_data,y_data = monthly_sales(df,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        name = 'Cohort 2'
                    )
                datas.append(data)
            elif val == 3:
                x_data,y_data = monthly_sales(df,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        name = 'Cohort 3'
                    )
                datas.append(data)
            elif val == 4:
                x_data,y_data = monthly_sales(df,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        name = 'Cohort 4'
                    )
                datas.append(data)
            elif val == 5:
                x_data,y_data = monthly_sales(df,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        name = 'All'
                    )
                datas.append(data)
        layout_sales['title'] = 'Monthly Sales Volume'
        layout_sales['margin'] = {'l': 45, 'r': 45, 'b': 70, 't': 35}
        layout_sales['xaxis'] = {'title': 'Months Year'}
        layout_sales['yaxis'] = {'title': 'Count'}
        fig = go.Figure(data=datas,layout=layout_sales)
        return fig
#Main
if __name__ == '__main__':
    app.run_server(debug=True)
