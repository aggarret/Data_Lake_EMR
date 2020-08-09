#!/bin/bash
aws s3 cp s3://s3-for-emr-cluster2/etl.py /home/hadoop/etl.py
aws s3 cp s3://s3-for-emr-cluster2/dl.cfg /home/hadoop/dl.cfg
# sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh
# sudo easy_install-3.6 pip
# sudo /usr/local/bin/pip3 install configparser
