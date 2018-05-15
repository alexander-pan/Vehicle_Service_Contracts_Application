import dash
from flask import Flask
from flask_caching import Cache

#Setup App
server = Flask(__name__)

app = dash.Dash(__name__,server=server)
app.css.append_css({"external_url":"static/dashboard.css"})
app.config.suppress_callback_exceptions = True
