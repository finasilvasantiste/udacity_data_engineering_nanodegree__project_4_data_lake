# Udacity Data Engineering Nanodegree
# Project 4: Data Lake

# Goal

Build an ETL pipeline that extracts data from S3, 
processes them using Spark, and loads the data back into S3 as a 
set of dimensional tables.

# Setup

## Config file
- Make a copy of the file `dl.cfg.sample` and rename it to `dl.cfg`.
- Replace the empty/indicated values with your credentials. 
  Make sure not to use any quotation marks.
  
## Dependencies
*Note: These are the dependencies needed to run the scripts
to create and delete aws resources. They do not include
dependencies needed to run `etl.py`. That file is deployed on 
the AWS EMR cluster which includes a Spark cluster and its dependencies.*
- Install the pipenv virtual environment by running `pip3 install pipenv`.
- Set up the virtual environment by navigating to the root folder
and running `pipenv install`.
- Make sure your python path is set correctly by running:
``export PYTHONPATH=$PATHONPATH:`pwd```

  
# How to run the app
- Create all aws resources needed to set up the EMR cluster by running `pipenv run python3 aws/create_aws_resources.py`.
- Delete all aws resources by running `pipenv run python3 aws/delete_aws_resources.py`.
- Deploy `etl.py` on the newly created EMR Cluster called `Boto3 Project 4 Cluster`.
