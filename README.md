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

# pipeline_ELT

* Instalando Pyspark ```pip install pyspark```
* ~https://spark.apache.org/downloads.html Spark 3.1.3 Hadoop 3.2~
* ~wget https://dlcdn.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz~
* https://archive.apache.org/dist/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz
* tar -xvzf spark-3.1.3-bin-hadoop3.2.tgz  
* ``` ./bin/spark-submit /home/alura/Documents/pipeline_ELT/pipeline_ELT/src/spark/transformation.py --src /home/alura/Documents/pipeline_ELT/pipeline_ELT/datalake/twitter_nbabrasil --dest /home/alura/Documents/pipeline_ELT/pipeline_ELT/src/spark/output --process-date 2022-08-15 ```
* pip install apache-airflow-providers-apache-spark

Configurando Aiflow Spark

* Admin/Connections
* Add
* Connection Type = Spark
* Host = local
* comando PWD na pasta do Spark
* {"spark-home": "/home/alura/Documents/spark-3.1.3-bin-hadoop3.2"}
* export SPARK_HOME=spark-3.1.3-bin-hadoop3.2

Testando Task

* airflow dags list
* airflow tasks list twitter_dag
* airflow tasks test twitter_dag transform_twitter_aluraonline 2022-08-15

Sempre que for usar um novo terminal

* source venv/bin/activate
* ```export AIRFLOW_HOME=$(pwd)/airflow_pipeline```
* airflow standalone
