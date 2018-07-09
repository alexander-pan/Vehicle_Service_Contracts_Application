FROM nginx:1.15

COPY flask.nginx /etc/nginx/sites-available/
COPY flask.nginx /etc/nginx/sites-enabled/
