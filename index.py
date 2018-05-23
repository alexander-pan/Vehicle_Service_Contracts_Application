#load libraries
import dash_html_components as html
import dash_core_components as dcc
import dash_table_experiments as dte
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State

from app import app
from apps import app3,app4,app5,app6,app7,app9,app10

home = html.Div([
    html.H3('Home Page'),
    dcc.Link('Admin Bank Reconcillation Summary', href='/apps/app3'),
    html.Br(),
    dcc.Link('Admin Bank Reconcillation Details', href='/apps/app4'),
    html.Br(),
    dcc.Link('Funder Bank Reconcillation Summary', href='/apps/app5'),
    html.Br(),
    dcc.Link('Funder Bank Reconcillation Details', href='/apps/app6'),
    html.Br(),
    dcc.Link('Scenario Modeling Summary', href='/apps/app7'),
    html.Br(),
    dcc.Link('Admin Scenario Modeling Details', href='/apps/app9'),
    html.Br(),
    dcc.Link('Vendor Scenario Modeling Details', href='/apps/app10')
])

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content'),
    html.Div(dte.DataTable(rows=[{}]), style={'display': 'none'})
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/apps/app3':
        return app3.layout_page
    elif pathname == '/apps/app4':
        return app4.layout_page
    elif pathname == '/apps/app5':
        return app5.layout_page
    elif pathname == '/apps/app6':
        return app6.layout_page
    elif pathname == '/apps/app7':
        return app7.layout_page
    elif pathname == '/apps/app9':
        return app9.layout_page
    elif pathname == '/apps/app10':
        return app10.layout_page
    else:
        return home

if __name__ == '__main__':
    app.run_server(host="0.0.0.0", debug=True)
