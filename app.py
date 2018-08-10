import dash
import dash_auth
from flask import Flask
from flask_caching import Cache

from flask_dance.contrib.google import make_google_blueprint, google

from auth.google_oauth import GoogleOAuth

import sys
sys.path.append('./static')
from authorized_emails import authorized_emails

#test local
import os
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
#authorized_emails = ["alex.pan@statusquota.co","avpan090@gmail.com"]

#Setup App
server = Flask(__name__)

app = dash.Dash(__name__,server=server,url_base_pathname='/')
auth = GoogleOAuth(app, authorized_emails)
app.css.append_css({"external_url":"static/dashboard.css"})
app.config.suppress_callback_exceptions = True

@server.route("/")
def MyDashApp():
    return app.index()
