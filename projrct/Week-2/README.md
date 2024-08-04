# Week 2: Data processing with Spark

In this step, you will perform analysis on the data with the aim to understand its structure, what transformations you need to generate insights that address the initial problem of our client.

## what you expect to learn:

1. Using PySpark for large scale data processing
2. Coding standards
3. Using Python notebooks
4. Setting GCP Permissions

## Pre-requisites

1. Install miniconda - https://docs.anaconda.com/free/miniconda/
    1. Create and activate the environment in `capstone.yml` - [how to](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
2. Create a python conda environment with python 3.9 - https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#creating-an-environment-with-commands
3. Install google cloud cli - https://cloud.google.com/sdk/docs/install
4. Set up access to GCP via [Service accounts](https://cloud.google.com/iam/docs/service-account-overview)
    1. See example notebook

## Tasks

1. Read data from google cloud storage
    1. Full documentation (column descriptions and types) of the dataset can be found on [Kaggle](https://www.kaggle.com/datasets/uom190346a/e-commerce-customer-behavior-dataset) and 
2. Describe data to understand the data types
    1. Hints
        1. https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.describe.html?highlight=describe#pyspark.sql.DataFrame.describe
3. Perform transformations that provide the following insights
    1. Customer segmentation
        1. Categorise customers based on their spending habits, demographics, loyalty and satisfaction
    2. Customer churn and retention
        1. Identify inactive customers that haven’t made a purchase in the last 30 days
        2. Identify recent customers that made a purchase within the last 7 days
4. Write your results back to separate bucket  in GCS.
5. Add CI pipeline to test if your code is of good quality ( Refer to week 1, Task 6)

## Resources

1. https://cloud.google.com/blog/topics/developers-practitioners/how-connect-cloud-sql-using-python-easy-way
