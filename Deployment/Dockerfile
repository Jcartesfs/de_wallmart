FROM python:3.6


ENV PATH_CONTAINER='/home/'
WORKDIR ${PATH_CONTAINER}
ADD app '/home/app/'
COPY app_flask.py '/home/'

 

# Variables de entorno

ENV GCP_PROJECT_ID='de-wallmart'


# Dependencias python3
RUN pip3 install --upgrade pip  && pip3 install -r /home/app/requirements.txt


RUN pip3 install Flask gunicorn
CMD exec gunicorn --bind :$PORT --timeout 0 app_flask:app