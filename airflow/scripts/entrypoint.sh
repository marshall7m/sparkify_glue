#!/usr/bin/env bash

TRY_LOOP="20"

# default variables
# give airflow a home
: "${AIRFLOW_HOME:="/usr/local/airflow"}"
# creates fernet key for secure airflow connection
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Local}Executor}"

# export AIRFLOW_HOME 
export AIRFLOW_HOME
export AIRFLOW__CORE__FERNET_KEY
export AIRFLOW__CORE__EXECUTOR

case "$1" in
  webserver)
    echo 'initialize airflow db'
    airflow initdb
    echo 'upgrade db'
    airflow upgradedb
    echo 'deleting previous connections and adding connections in airflow:'
    bash /scripts/connections.sh
    airflow scheduler &
    exec airflow webserver -p 8080
    ;;
  worker|scheduler)
    sleep 10
    exec airflow "$@"
    ;;
  flower)
    sleep 10
    exec airflow "$@"
    ;;
  version)
    exec airflow "$@"
    ;;
  *)
    exec "$@"
    ;;
esac

