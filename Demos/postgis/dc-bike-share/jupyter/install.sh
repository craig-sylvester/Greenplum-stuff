#!/usr/bin/env bash

which conda &> /dev/null
[[ $? == 1 ]] && { echo "conda not found. Install with 'sudo yum -y install conda'"; exit 1; }

CONDA_ENV=$HOME/conda-sql

conda create -p ${CONDA_ENV}
conda init bash

conda activate ${CONDA_ENV}

conda update conda
conda install -c anaconda psycopg2
conda install -c anaconda sqlalchemy
pip install ipython-sql
