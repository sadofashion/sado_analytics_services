##
#  Generic dockerfile for dbt image building.
#  See README for operational details
##

ARG build_for=linux/amd64

FROM --platform=$build_for python:3.9.10-slim-bullseye as base

# Set docker basics
WORKDIR /usr/app/dbt/

RUN apt-get update -y && \
  apt-get install --no-install-recommends -y -q \
  git libpq-dev curl && \
  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Downloading gcloud package
RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz

# Installing the package
RUN mkdir -p /usr/local/gcloud \
  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
  && /usr/local/gcloud/google-cloud-sdk/install.sh

# Adding the package path to local
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin


RUN gcloud auth login 

COPY requirements.txt .

# Add dbt_project_1 to the docker image

COPY . .
RUN python -m pip install -r requirements.txt

RUN ["dbt", "deps", "--project-dir", "$PWD"]


