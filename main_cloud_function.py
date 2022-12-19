import argparse
import os
import re
from logging import exception
from google.cloud import dataproc_v1
from google.cloud import storage

# region Variables
pyspark_file = "gs://"
waiting_callback = False
project_id = ""
region = "us-central1"
zone = "us-central1-a"
cluster_name = ""

# Create the cluster client.
cluster_client = dataproc_v1.ClusterControllerClient(
    client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
)

# endregion

# region CREATE CLUSTER
def quickstart(project_id, region, cluster_name, pyspark_file):
    # Create the cluster config.
    print("---------------INFO: CREATING CLUSTER---------------------")
    cluster = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 4, "machine_type_uri": "n1-standard-2"},
        },
    }

    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": project_id, "region": region, "cluster": cluster}
    )
    result = operation.result()

    print(
        "---------------INFO: CLUSTER CREATED SUCCESSFULLY: {}".format(
            result.cluster_name
        )
        + " ---------------------"
    )
    # [END dataproc_create_cluster]


# endregion

# region [START dataproc_submit_job]
def submit_job(project_id, region, cluster_name, pyspark_file):

    # Create the job client.
    print("---------------INFO: SUBMITING JOB---------------------")
    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": "{}-dataproc.googleapis.com:443".format(region)}
    )

    # Create the job config.
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": pyspark_file,
            "jar_file_uris": [
                "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
            ],  #!!!!!!!!!IMPORTANT: If you want to use bigquery with pyspark, you MUST include this librarie.
        },
    }

    operation = job_client.submit_job_as_operation(
        request={"project_id": project_id, "region": region, "job": job}
    )
    response = operation.result()

    # Dataproc job output is saved to the Cloud Storage bucket
    # allocated to the job. Use regex to obtain the bucket and blob info.
    matches = re.match("gs://(.*?)/(.*)", response.driver_output_resource_uri)

    output = (
        storage.Client()
        .get_bucket(matches.group(1))
        .blob(f"{matches.group(2)}.000000000")
        .download_as_string()
    )

    print(
        f"---------------INFO: JOB FINISHED SUCCESSFULLY: {output}\r\n--------------------"
    )

    # [END dataproc_submit_job]


# endregion#

# region # [START dataproc_delete_cluster]
def delete_cluster(project_id, region, cluster_name):

    # Delete the cluster once the job has terminated.
    print("---------------INFO: DELETING CLUSTER---------------------")
    operation = cluster_client.delete_cluster(
        request={
            "project_id": project_id,
            "region": region,
            "cluster_name": cluster_name,
        }
    )
    operation.result()

    print("Cluster {} successfully deleted.".format(cluster_name))
    print(
        "---------------INFO: CLUSTER{} SUCCESSFULLY DELETED.".format(cluster_name)
        + " ---------------------"
    )


# [END dataproc_delete_cluster]
# endregion


def start_heavy_load(context):
    try:
        quickstart(project_id, region, cluster_name, pyspark_file)
        submit_job(project_id, region, cluster_name, pyspark_file)
        delete_cluster(project_id, region, cluster_name)
    except:
        print(
            "---------------INFO: AN ERROR HAPPENED, PLEASE CHECK LOGS TO HAVE MORE INFORMATION---------------------"
        )
