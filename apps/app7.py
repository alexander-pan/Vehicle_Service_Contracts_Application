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
import plotly.graph_objs as go

#Multi-Dropdown options
from controls import SELLERS, FUNDERS, TXCODES


#Get initial Dataframe to work with
DF = pd.read_pickle('./static/data/Scenario_Modeling_INFO.pkl')
DF_PER = pd.read_pickle('./static/data/Funding_Fee_Percents.pkl')
DF_TXLOG = pd.read_pickle('./static/data/TXLog_Cashflows.pkl')

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
seller_options.append({'label': 'ALL SELLERS', 'value': 1})

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
            html.H1('Scenario Modeling Dashboard',
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
                        id='funder',
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
                        id='seller',
                        options=seller_options,
                        placeholder='Select a Seller',
                        multi=True
                    )
                ]),
            ],
            className='six columns',style={'display':'block'}
        ),
    ],style={'width':'100%','display':'inline-block'}),

    #First Row of Values
    html.Div([
        html.Div(
            [
                dcc.Textarea(
                    id='n_contracts',
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
                html.Label('Total Contracts Sold',
                    style={'fontSize':15,
                           'textAlign':'center'}),

            ],
            className='four columns'
        ),

        html.Div(
            [
                dcc.Textarea(
                    id='face_value',
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
                html.Label('Total Face Value Sold',
                    style={'fontSize':15,
                           'textAlign':'center'}),

            ],
            className='six columns'
        ),

        html.Div(
            [
                dcc.Textarea(
                    id='avg_sold_month',
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
            className='two columns'
        ),
    ],style={'width':'100%','display':'inline-block'}),

    #Second Row of Values
    html.Div([
        html.Div(
            [
                dcc.Textarea(
                    id='avg_face_value',
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
                html.Label('Avg. Face Value',
                    style={'fontSize':15,
                           'textAlign':'center'}),

            ],
            className='four columns'
        ),

        html.Div(
            [
                dcc.Textarea(
                    id='seller_adv_rec',
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
                html.Label('Total Seller Advance',
                    style={'fontSize':15,
                           'textAlign':'center'}),

            ],
            className='six columns'
        ),

        html.Div(
            [
                dcc.Textarea(
                    id='avg_sold_3month',
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
                html.Label('Avg. Contracts Sold, 3 Months',
                    style={'fontSize':15,
                           'textAlign':'center'}),

            ],
            className='two columns'
        ),
    ],style={'width':'100%','display':'inline-block'}),

    #Third Row of Values
    html.Div([
        html.Div(
            [
                html.Div(
                    [
                        dcc.Textarea(
                            id='growth_rate',
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
                        html.Label('Growth Rate, M-O-M (Recent,Complete)',
                            style={'fontSize':15,
                                   'textAlign':'center'}),

                    ],
                    className='three columns'
                ),

                html.Div(
                    [
                        dcc.Graph(id='monthly_sales',figure={'layout':layout})
                    ],
                    className='nine columns',
                    style={'display': 'inline-block'},
                ),
            ],
            #style={'margin-top':'20','margin-left':'20'}
        ),
    ],style={'width':'100%','display':'inline-block'}),

    #Final Row of Values
    html.Div([
        html.Div(
            [
                html.Div(
                    [
                        dcc.Checklist(
                            id = 'curve_list',
                            options = [
                                {'label': 'Cohort 1(1-6)','value': 1},
                                {'label': 'Cohort 2(7-12)','value': 2},
                                {'label': 'Cohort 3(13-15)','value': 3},
                                {'label': 'Cohort 4(16-18)','value':4},
                                {'label': 'Cohort 5(19-24)','value':5},
                                {'label': 'All (1-24)','value':6}
                            ],
                            values=[6],
                            labelStyle={'display':'inline-block'}
                        ),
                        dcc.Graph(id='cancellation_curve',figure={'layout':layout})
                    ],
                    className='twelve columns',
                    #style={'width':'90%','display': 'inline-block'},
                ),
            ],
        ),
    ],style={'width':'90%','display':'inline-block'}),
#ending of layout
],style={'margin-left':40,'margin-right':40})

#FUNCTIONS
@cache.memoize()
def getCohort(df,seller,funder):
    dataframe = []
    for vendor in seller:
        for fundee in funder:
            dataframe.append(df.loc[(df.SellerName==vendor) & (df.FundCo==fundee)])
    return pd.concat(dataframe)

@cache.memoize()
def createCancelCurve(dataframe,cohort):
    df = dataframe.copy()

    if cohort == 1:
        df = dataframe.loc[(dataframe.Installments >= 1) & (dataframe.Installments <= 6)]
    elif cohort == 2:
        df = dataframe.loc[(dataframe.Installments >= 7) & (dataframe.Installments <= 12)]
    elif cohort == 3:
        df = dataframe.loc[(dataframe.Installments >= 13) & (dataframe.Installments <= 15)]
    elif cohort == 4:
        df = dataframe.loc[(dataframe.Installments >= 16) & (dataframe.Installments <= 18)]
    elif cohort == 5:
        df = dataframe.loc[(dataframe.Installments >= 19) & (dataframe.Installments <= 24)]

    #Find current cancellation curves percentages
    tupes = zip(range(0,25),range(4,29))
    payments,cancel_percent = [],[]

    for i,j in tupes:
        qfc_date = (dt.now()+relativedelta(months=-j)).date()
        num = float(df.loc[(df['IsCancelled']==1)
                    & (df['PaymentsMade']==i)
                    & (df['EffectiveDate'] < qfc_date)].shape[0])
        den = float(df.loc[(df['EffectiveDate'] < qfc_date)].shape[0])

        if den != 0:
            payments.append(i+1)
            cancel_percent.append(num/den*100.0)
        else:
            payments.append(i+1)
            cancel_percent.append(np.nan)
    return payments,cancel_percent

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
    elif output == 'growth_rate':
        #count how many contracts there are in "month year"
        for date in sorted(df['EffectiveDate']):
            key = date.strftime('%b %y')
            if key in months:
                months[key] +=1
            else:
                months[key] = 1

        #looking back at the start of 2 months from now, get n contracts for last 2 completed
        #full months entered in database.
        #Ex. if now= April 2018, we look at Jan. 2018 and Feb 2018.
        #we calculate the growth rate from past month to current month
        complete = False
        i = -2
        j = -1
        while not complete:
            last_m = [dt.strptime(x,'%b %y') for x in months.keys()[i:]]
            values = [x for x in months.values()[i:]]

            if last_m[j].strftime('%b %y') == (dt.now()-relativedelta(months=1)).strftime('%b %y'):
                i += -1
                j += -1
            else:
                rate = 1.0*(values[1] - values[0])/values[0]
                complete = True
        return round(rate*100,2)
    elif output == "plot_cancelled":
        #get cancelled contracts only
        df_cancel = df.loc[df.IsCancelled==1]

        #count how many contracts "month year" for cancelled
        for date in sorted(df_cancel['EffectiveDate']):
            key = date.strftime('%b %y')
            if key in months:
                months[key] += 1
            else:
                months[key] = 1
        label = [x[0] for x in months.iteritems()]
        count = [x[1] for x in months.iteritems()]
        return label,count
    elif output == "plot_open":
        #get open contracts only
        df_open = df.loc[(df.IsCancelled==0) & (df.PaymentsRemaining!=0)]

        #count how many contracts "month year" for open
        for date in sorted(df_open['EffectiveDate']):
            key = date.strftime('%b %y')
            if key in months:
                months[key] += 1
            else:
                months[key] = 1
        label = [x[0] for x in months.iteritems()]
        count = [x[1] for x in months.iteritems()]
        return label,count
    elif output == "plot_complete":
        #get completed contracts only
        df_complete = df.loc[(df.PaymentsRemaining==0)]

        #count how many contracts "month year" for completed contracts
        for date in sorted(df_complete['EffectiveDate']):
            key = date.strftime('%b %y')
            if key in months:
                months[key] += 1
            else:
                months[key] = 1
        label = [x[0] for x in months.iteritems()]
        count = [x[1] for x in months.iteritems()]
        return label,count
    else:
        #get all contracts
        return df.shape[0]

#callbacks to update values in layout 1
@app.callback(Output('n_contracts','value'),
              [Input('funder','value'),
              Input('seller','value')])
def update_AvgContracts(funder,seller):
    if ((funder is not None) and (seller is not None)):
        if 1 in seller:
            seller = [SELLERS[x] for x in SELLERS]
        dataframe = getCohort(DF,seller,funder)
        print seller
        return MonthlySales(dataframe,'total')

@app.callback(Output('face_value','value'),
              [Input('funder','value'),
              Input('seller','value')])
def update_FaceValue(funder,seller):
    if ((funder is not None) and (seller is not None)):
        if 1 in seller:
            seller = [SELLERS[x] for x in SELLERS]
        dataframe = getCohort(DF,seller,funder)
        return '${:,.0f}'.format(round(dataframe.AmountFinanced.sum(),2))

@app.callback(Output('avg_sold_month','value'),
              [Input('funder','value'),
              Input('seller','value')])
def update_AvgMonth(funder,seller):
    if ((funder is not None) and (seller is not None)):
        if 1 in seller:
            seller = [SELLERS[x] for x in SELLERS]
        dataframe = getCohort(DF,seller,funder)
        return MonthlySales(dataframe,'mean')

@app.callback(Output('avg_face_value','value'),
              [Input('funder','value'),
              Input('seller','value')])
def update_AvgFaceValue(funder,seller):
    if ((funder is not None) and (seller is not None)):
        if 1 in seller:
            seller = [SELLERS[x] for x in SELLERS]
        dataframe = getCohort(DF,seller,funder)
        return '${:,.0f}'.format(round(dataframe.AmountFinanced.mean(),2))

@app.callback(Output('seller_adv_rec','value'),
              [Input('funder','value'),
              Input('seller','value')])
def update_SellerAdvRec(funder,seller):
    if ((funder is not None) and (seller is not None)):
        if 1 in seller:
            seller = [SELLERS[x] for x in SELLERS]
        dataframe = getCohort(DF,seller,funder)
        return '${:,.0f}'.format(round(dataframe.SellerAdvanceAmount.sum(),2))

@app.callback(Output('avg_sold_3month','value'),
              [Input('funder','value'),
              Input('seller','value')])
def update_AvgSold3Months(funder,seller):
    if ((funder is not None) and (seller is not None)):
        if 1 in seller:
            seller = [SELLERS[x] for x in SELLERS]
        dataframe = getCohort(DF,seller,funder)
        return MonthlySales(dataframe,'mean_3months')

@app.callback(Output('growth_rate','value'),
              [Input('funder','value'),
              Input('seller','value')])
def update_GrowthRate(funder,seller):
    if ((funder is not None) and (seller is not None)):
        if 1 in seller:
            seller = [SELLERS[x] for x in SELLERS]
        dataframe = getCohort(DF,seller,funder)
        return '{:,.0f}%'.format(MonthlySales(dataframe,'growth_rate'))

@app.callback(Output('monthly_sales','figure'),
              [Input('funder','value'),
              Input('seller','value')])
def update_MonthlySales(funder,seller):
    if ((funder is not None) and (seller is not None)):
        if 1 in seller:
            seller = [SELLERS[x] for x in SELLERS]
        dataframe = getCohort(DF,seller,funder)
        layout_sales= copy.deepcopy(layout)
        x1_data,y1_data = MonthlySales(dataframe,'plot_complete')
        x2_data,y2_data = MonthlySales(dataframe,'plot_cancelled')
        x3_data,y3_data = MonthlySales(dataframe,'plot_open')
        data1 = go.Bar(
                x =  x1_data,
                y = y1_data,
                marker=dict(color='rgba(50, 171, 96, 0.7)'),
                name = 'Completed'
            )
        data2 = go.Bar(
                        x =  x2_data,
                        y = y2_data,
                        marker=dict(color='rgba(219, 64, 82, 0.7)'),
                        name = 'Cancelled'
                    )
        data3 = go.Bar(
                        x =  x3_data,
                        y = y3_data,
                        marker=dict(color='rgba(55, 128, 191, 0.7)'),
                        name = 'Open'
                    )
        layout_sales['title'] = "Cohort's Monthly Sales Volume"
        layout_sales['margin'] = {'l': 45, 'r': 45, 'b': 70, 't': 35}
        layout_sales['xaxis'] = {'title': 'Months Year'}
        layout_sales['yaxis'] = {'title': 'Count'}
        layout_sales['barmode'] = 'relative'
        fig = go.Figure(data=[data2,data3,data1],layout=layout_sales)
        return fig

@app.callback(Output('cancellation_curve','figure'),
              [Input('funder','value'),
              Input('seller','value'),
              Input('curve_list','values')])
def update_cancel_curve(funder,seller,curve_list):
    if ((funder is not None) and (seller is not None)):
        if 1 in seller:
            seller = [SELLERS[x] for x in SELLERS]
        dataframe = getCohort(DF,seller,funder)
        layout_curve = copy.deepcopy(layout)

        #Overall Portfolio Data
        X, y = createCancelCurve(DF,6)
        data = go.Bar(
                x =  X,
                y = y,
                text = [int(round(x)) for x in y],
                textposition='outside',
                name = 'Overall'
            )
        datas = [data]

        #Add cohort if any
        for val in curve_list:
            if val == 1:
                x_data,y_data = createCancelCurve(dataframe,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        text = [int(round(x)) if ~np.isnan(x) else 0 for x in y_data],
                        textposition='outside',
                        opacity = 1,
                        name = 'Cohort(1-6)'
                    )
                datas.append(data)
            elif val == 2:
                x_data,y_data = createCancelCurve(dataframe,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        text = [int(round(x)) if ~np.isnan(x) else 0 for x in y_data],
                        textposition='outside',
                        opacity = 1,
                        name = 'Cohort(7-12)'
                    )
                datas.append(data)
            elif val == 3:
                x_data,y_data = createCancelCurve(dataframe,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        text = [int(round(x)) if ~np.isnan(x) else 0 for x in y_data],
                        textposition='outside',
                        opacity = 1,
                        name = 'Cohort(13-15)'
                    )
                datas.append(data)
            elif val == 4:
                x_data,y_data = createCancelCurve(dataframe,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        text = [int(round(x)) if ~np.isnan(x) else 0 for x in y_data],
                        textposition='outside',
                        opacity = 1,
                        name = 'Cohort(16-18)'
                    )
                datas.append(data)
            elif val == 5:
                x_data,y_data = createCancelCurve(dataframe,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        text = [int(round(x)) if ~np.isnan(x) else 0 for x in y_data],
                        textposition='outside',
                        opacity = 1,
                        name = 'Cohort(19-24)'
                    )
                datas.append(data)
            elif val == 6:
                x_data,y_data = createCancelCurve(dataframe,val)
                data = go.Bar(
                        x =  x_data,
                        y = y_data,
                        text = [int(round(x)) if ~np.isnan(x) else 0 for x in y_data],
                        textposition='outside',
                        opacity = 1,
                        name = 'Vendor(All)'
                    )
                datas.append(data)
        layout_curve['title'] = "Cohort's Current Cancel Rates per Payments Made"
        layout_curve['xaxis'] = {'title': 'Payments Made'}
        layout_curve['yaxis'] = {'title': 'Cancellation % Rate '}
        layout_curve['height'] = 400
        fig = go.Figure(data=datas,layout=layout_curve)
        return fig
