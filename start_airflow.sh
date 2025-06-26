#!/bin/bash

# start webserver
airflow webserver --port 8080 &

# start scheduler
airflow scheduler 
