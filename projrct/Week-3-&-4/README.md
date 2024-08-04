# Week 3&4: Orchestration with Airflow on GCP cloud composer

In this step, we want to automate the insights generation(as done in previous weeks) as a task that is scheduled to run daily at 10am using Airflow - Google cloud composer.

## What you will learn:
1. Orchestration and scheduling of periodic tasks with Airflow
2. Construct [DAGs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
3. Synchronize DAGs to Google Cloud composer with Github Actions
4. Manage CI environment variables and secrets in Github

### Pre-requisites

1. Established access to Google cloud composer. This is already set up
2. Understanding of [service accounts](https://cloud.google.com/iam/docs/service-account-overview), Cloud storage connection to composer and Github Actions


### Tasks:

1. Define your DAG
    1. Set your schedule to 10:00 am daily
    2. Add labels and description to provide more information about what the DAG does.
2. Add your insights specification from the previous step. Modularise as Python functions
    1. Hints: 
        1. Use Python Operator to ensure your code can run <- check right operator
        2. Ensure your DAG reads/accesses the right data
        3. Write your insights back to the same bucket in a sub folder - `gs://<BUCKET>/insights`
        4. It is possible to install any Python dependencies - [how to](https://cloud.google.com/composer/docs/composer-3/install-python-dependencies)

3. Integrate the sync of DAGs with your CI/CD - (See Week 1 Task 6)
    1. Hints: 
        1. See resource #2
        2. Ensure your CI environment has the required credentials to access the GCP account
        3. CI/CD should copy you DAG files to the DAG folder in the bucket connected to composer. See resource #3
    


## Resources

1. Python functions
    1. https://realpython.com/defining-your-own-python-function/
2. Google cloud composer - This is managed Airflow provided by GCP
    1. https://cloud.google.com/composer/docs/composer-2/manage-dags#console_1
3. Github Actions to Upload files to Google Cloud Storage
    1. https://github.com/google-github-actions/upload-cloud-storage
4. Airflow Basics
    1. https://medium.com/dev-genius/orchestrating-data-engineering-tasks-with-airflow-edc38e42fda1
    2. https://airflow.apache.org/docs/apache-airflow/stable/start.html
5. Using Spark in Airflow
    1. https://medium.com/codex/executing-spark-jobs-with-apache-airflow-3596717bbbe3
