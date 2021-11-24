# Udacity Data Engineering Nanodegree
# Project: Data Lake

# Goal

Build an ETL pipeline that extracts data from S3, 
processes them using Spark, and loads the data back into S3 as a 
set of dimensional tables.

# Setup

## Config file
- Make a copy of the file `dl.cfg.sample` and call it just `dl.cfg`.
- Replace the empty/indicated values with your credentials, and leave the filled out values as they are. 
  Make sure not to use any quotation marks.
  
## Dependencies
- Install the pipenv virtual environment by running `pip3 install pipenv`.
- Set up the virtual environment by navigating to the root folder
and running `pipenv install`.
- Make sure your python path is set correctly by running:
``export PYTHONPATH=$PATHONPATH:`pwd```

## How to run the app
- Create all aws resources needed to set up redshift by running `pipenv run python3 aws/create_aws_resources.py`.
  