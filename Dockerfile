FROM python:3.9-slim

WORKDIR /app

COPY . /app


RUN pip install -r requirements.txt

RUN pip install python-dotenv   

EXPOSE 80


ENV DOTENV_PATH=/app/env_config  


ENTRYPOINT ["python", "user_logins_etl.py"]
