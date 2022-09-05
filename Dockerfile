FROM python:3.8

WORKDIR /home/wiwi

RUN sudo mkdir /home/wiwi; exit 0 

COPY . /home/wiwi

RUN pip install -r requirements.txt

CMD [ "python", "./main.py" ]