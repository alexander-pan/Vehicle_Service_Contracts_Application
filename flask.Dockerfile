FROM ubuntu:16.04

RUN apt-get update \
    && apt-get install python-dev -y \
    && apt-get install build-essential -y\
    && apt-get install python-pip -y \
    && apt-get install vim -y \
    && apt-get install host -y \
    && pip install --upgrade pip 

WORKDIR /root/

RUN apt-get install unixodbc-dev -y

COPY requirements.txt /root/requirements.txt

RUN pip install -r requirements.txt  

EXPOSE 8050
