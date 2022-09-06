# FreeRadius
FROM freeradius/freeradius-server:latest
COPY raddb/ /etc/raddb/


# Python stuff
FROM python:3.8

WORKDIR /home/wiwi

RUN sudo mkdir /home/wiwi; exit 0 

COPY . /home/wiwi

RUN --mount=type=cache,target=/root/.cache pip install -r requirements.txt

CMD [ "python", "./main.py" ]