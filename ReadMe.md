# Sparkify Data Lake with Amazon Web Services and pyspark.


# Introduction

In this project, we will build a data lake for a fictitious online data streaming company called Sparkify.  _A data lake is a system or repository of data stored in its natural/raw format, usually, object blobs or files. A data lake is usually a single store of all enterprise data including raw copies of source system data and transformed data used for tasks such as reporting, visualization, advanced analytics, and machine learning<sup> [1] </sup>._

Currently, our data is stored in 2 different files containing JSON data from users.  One file contains our user’s data(log_data) and the other file contains the metadata associated with the songs(song_data).  In order for the Sparkify Analytics team to get actionable insight out of the data, we will store the data in HDFS [Parquet](https://acadgild.com/blog/parquet-file-format-hadoop#:~:text=Parquet%2C%20an%20open%20source%20file,in%20any%20Hadoop%20ecosystem%20like) files on an AWS S3 bucket. In order to complete this task, we will have to complete the task below.



1. [Setup for EMR cluster](#setup-for-emr-cluster)
2. [Upload our files to an S3 Bucket ](#upload-our-files-to-an-s3-bucket)
3. [Create the EMR Cluster](#create-the-emr-cluster)
4. [Add Python 3 to pyspark EMR cluster](#add-python-3-to-pyspark-emr-cluster)
5. [Submit spark job](#submit-spark-job)

# Setup for EMR cluster

In order to prepare our cluster, we have to go through some setup steps.  Fortunately, Tran Nguyen has created a [helpful guide](https://towardsdatascience.com/how-to-create-and-run-an-emr-cluster-using-aws-cli-3a78977dc7f0#c46c) that we can use for our initial set up.  We will only be completing steps 1-6 and step 8 in her guide, as well. There are minor differences that I will list below.

For our Pyspark script to work we will have to create an s3 bucket to store the files, so for the purposes of this ReadMe, this step is not optional. Make sure to change the name of the s3 bucket for the output file in etl.py after creating it.  In the code of etl.py change the name of the bucket to your bucket name that you created in step 4 of Tran’s guide.  Switch _s3-for-emr-cluster2_ to your cluster name. See the code below.

```python
output_data = "s3a://s3-for-emr-cluster2"
```

Once you’ve created your cluster in the next step, you will have to complete step 8 from Tran's guide.  On my AWS I only had to complete this one time on the first cluster I created.  All subsequent clustered had step 8 completed already.




# Upload Files to S3 Bucket

 In order for an EMR cluster to have access to our pyspark script (etl.py), configuration (dl.cfg), and bash file, we must first load them onto our s3 bucket.  I have found the best way to do this is to create a bash script to update/add all of our necessary files.  If you are on a windows machine, please install [Ubuntu bash for windows 10](https://altis.com.au/installing-ubuntu-bash-for-windows-10/) to run the [bash script](https://ryanstutorials.net/bash-scripting-tutorial/bash-script.php) (bash2.sh).

The bash2.sh contains 3 lines of code.
```bash
aws s3 cp /home/alfred/Desktop/Data_Lake/bash.sh s3://s3-for-emr-cluster2/
```
bash file for our bootstrap to be run when our cluster is being initialized  

```bash
aws s3 cp /home/alfred/Desktop/Data_Lake/etl.py s3://s3-for-emr-cluster2/
```
Pyspark that will tell the cluster how to process our JSON data and store our parquet files.

```bash
CODE: aws s3 cp /home/alfred/Desktop/Data_Lake/dl.cfg s3://s3-for-emr-cluster2/
```

The configuration file to store the user key and secret identification.  

To run this, well will run “$bash bash2.sh” from the command line and all files will be uploaded to the s3 bucket with any saved changes.

**Please Note**: Change **s3://s3-for-emr-cluster2/ **with your cluster name in the above bash file, you must do the same thing with the bash.sh file that will pull our files from the s3 bucket into our EMR Cluster.  

**Update** the dl.cfg file with your IAM User **ACCESS KEY ID** and **SECRET ACCESS KEY**.




# Create the EMR Cluster

In this step, we will build our EMR cluster.  We will use the create-cluster command through the AWS CLI. _Amazon EMR is the industry-leading cloud big data platform for processing vast amounts of data using open source tools such as Apache Spark, Apache Hive, Apache HBase, Apache Flink, Apache Hudi, and Presto<sup>[2]</sup>_. Below is a copy of the code you will enter in your terminal to create your cluster.

```bash
aws emr create-cluster --name test-emr-cluster --use-default-roles --release-label emr-5.28.0 --instance-count 3 --instance-type m5.xlarge --applications Name=Spark Name=Hadoop --ec2-attributes KeyName=spark-cluster --log-uri s3://s3-for-emr-cluster2/ --bootstrap-actions Path=s3://s3-for-emr-cluster2/bash.sh
```

Below is a full explanation for the different AWS create cluster options.



*   Full AWS create cluster command options
    *   --name
        *   The name of the cluster
    *   --use-default-roles
        *   Default EMR role
    *   --release-label
        *   Specifies the cluster version
    *   --instance-count
        *   Specifies the number of nodes
    *   --instance-type
        *   Specifies the type of node
    *   --applications
        *   Name=Spark
            *   Installs Spark
        *   Name=Hadoop
            *   Installs Hadoop
    *   --log-uri
        *   Specifies location for the EMR cluster
    *   --bootstrap-actions
        *   Path=s3://s3-for-emr-cluster2/bash.sh
            *   Specifies the file location to be loaded to the cluster  

	Please note: If this is your first cluster, please complete step 8. Additionally, the path to the bash files in the s3 bucket needs to be changed to your bash files under bootstrap-actions.  Below is an explanation for the code in the bootstrap file.  Also, change the path of the log-uri to your bucket location.   



*   aws s3 cp s3://s3-for-emr-cluster2/etl.py /home/hadoop/etl.py
    *   Copy etl.py to the Hadoop cluster
*   aws s3 cp s3://s3-for-emr-cluster2/dl.cfg /home/hadoop/dl.cfg
    *   Copy dl.cfg to the Hadoop cluster


# Add Python 3 to pyspark EMR cluster

First, log into the EMR cluster.  See code below:
```bash
aws emr ssh --cluster-id {your cluster id} --key-pair-file ~/.aws/{pem file name}
```

Since the etl.py script is written in Python 3, we will have to upgrade our python installation on our cluster to python 3.  By default, the EMR cluster comes with python 2.7 installed.  We will need to download python 3 and then install the configparser via pip.


```bash
sudo sed -i -e '$a\export PYSPARK_PYTHON=/usr/bin/python3' /etc/spark/conf/spark-env.sh
```

Run pyspark with python 3

```bash
sudo easy_install-3.6 pip
```

Pip 3.6

```bash
sudo /usr/local/bin/pip3 install configparser
```
Install the configparser library




# Submit spark job

Our final step is to submit our spark job to our cluster and then finally to terminate the cluster.  Please be sure connected to the EMR cluster still. We will run the code below:
```bash
spark-submit --master yarn ./etl.py
```

The script will take about 30 minutes to run.  After the script is done, the final step is to terminate the cluster to avoid any additional changes.    

```bash
aws emr terminate-clusters --cluster-id {your cluster id}
```
# Notes
<sup>1</sup>["Data Lake - Wikipedia"](https://en.wikipedia.org/wiki/Data_lake#:~:text=A%20data%20lake%20is%20usually,advanced%20analytics%20and%20machine%20learning.)

<sup>2</sup> ["AWS EMR - Amazon Web Services"](https://aws.amazon.com/emr/?whats-new-cards.sort-by=item.additionalFields.postDateTime&whats-new-cards.sort-order=desc)
