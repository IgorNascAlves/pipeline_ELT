# pipeline_EL

Preparando Ubunutu

* sudo apt install update
* sudo apt install upgrade
* sudo apt install build-essential gcc make perl dkms curl tcl
* Instalar o vscode pela Ubuntu Software e extensão do python

Ambiente virtual

* sudo add-apt-repository ppa:deadsnakes/ppa
* sudo apt install python3.9
* sudo apt install python3.9-venv
* mkdir Documentos/pipeline_EL
* cd Documentos/pipeline_EL
* python3.9 -m venv venv
* source venv/bin/activate



Comandos para a instalação AIRFLOW
* ```sudo apt install python3-pip```

* ```AIRFLOW_VERSION=2.3.2```
* ```PYTHON_VERSION=3.9```
* ```CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"```
* ```pip install "apache-airflow[postgres,celery,redis]==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"```

Inicializando Airflow

* ```export AIRFLOW_HOME=$(pwd)/airflow_pipeline``` (Verificar se está na pasta do projeto)
* ```airflow standalone```

Removendo exemplos

* Abrir airflow.cfg
* load_examples = False

Conectando Hook

* Admin/Connections
* Add
* Connection Id = twitter_default
* Connection Type = Http
* Host = https://api.twitter.com
* Extra = {"Authorization": "Bearer XXXXXXXXXXXXXXXXXXXXXX"}