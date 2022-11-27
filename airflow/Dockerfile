ARG debian_buster_image_tag=8-jre-slim
ARG spark_version="3.0.0"
FROM openjdk:${debian_buster_image_tag}
RUN apt-get update -y
RUN apt-get install -y curl python3 r-base
RUN apt-get update -y
RUN apt-get install -y python3-pip python3-dev
RUN pip install --upgrade pip
RUN pip install wget==3.2 pyspark
RUN pip install pandas

# pull official base image

# FROM python:3.9.13-slim-buster

RUN chmod 1777 /tmp


RUN mkdir -p /opt/workspace

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install psycopg2, git
RUN apt-get update \
    && apt-get -y install libpq-dev gcc gosu git \
    && pip install psycopg2

# install dependencies
RUN pip install --upgrade pip
COPY ./requirements.txt .
RUN pip install -r requirements.txt
RUN pip install --upgrade sqlalchemy

# set work directory
WORKDIR /opt/airflow

# copy project
COPY . .
