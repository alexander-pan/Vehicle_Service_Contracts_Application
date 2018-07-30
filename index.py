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
    dcc.Link('Admin Bank Reconcillation Summary', href='/abr_summary'),
    html.Br(),
    dcc.Link('Admin Bank Reconcillation Details', href='/abr_details'),
    html.Br(),
    dcc.Link('Funder Bank Reconcillation Summary', href='/fbr_summary'),
    html.Br(),
    dcc.Link('Funder Bank Reconcillation Details', href='/fbr_details'),
    html.Br(),
    dcc.Link('Scenario Modeling Summary', href='/scenario_summary'),
    html.Br(),
    dcc.Link('Admin Scenario Modeling Details', href='/adminscenario_details'),
    html.Br(),
    dcc.Link('Vendor Scenario Modeling Details', href='/vendorscenario_details')
])

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    html.Div(id='page-content'),
    html.Div(dte.DataTable(rows=[{}]), style={'display': 'none'})
])


@app.callback(Output('page-content', 'children'),
              [Input('url', 'pathname')])
def display_page(pathname):
    if pathname == '/abr_summary':
        return app3.layout_page
    elif pathname == '/abr_details':
        return app4.layout_page
    elif pathname == '/fbr_summary':
        return app5.layout_page
    elif pathname == '/fbr_details':
        return app6.layout_page
    elif pathname == '/scenario_summary':
        return app7.layout_page
    elif pathname == '/adminscenario_details':
        return app9.layout_page
    elif pathname == '/vendorscenario_details':
        return app10.layout_page
    else:
        return home

if __name__ == '__main__':
    app.run_server(host="0.0.0.0", debug=True)
