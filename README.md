# dataproc-cloud-function
Script to create a cluster in dataproc, submit a job and delete cluster  

**Cloud Function:**  
1.- Create cloud function:  
  ![image](https://user-images.githubusercontent.com/63972784/208485173-cf096e17-2074-4a48-8e2f-e1d790801500.png)  
  
  Set the "Timeout" to 540s, this due to the time gcp takes to create the cluster, depending on your script will also increase or decrease the time of execution and finally the time it takes to delete the cluster:      
  ![image](https://user-images.githubusercontent.com/63972784/208483961-4841fc6b-32ae-4d26-9a52-e33b021d6415.png)  
  
  Choose Python 3.7 as Runtime, paste main_cf.py code and make sure start_heavy_load is set as Entry Point:  
  ![image](https://user-images.githubusercontent.com/63972784/208484480-5cb1571c-8b44-4164-ad0d-fb1f57d124c6.png)  
  
  Add **google-cloud-dataproc** as librarie in the requirements.txt file:  
  ![image](https://user-images.githubusercontent.com/63972784/208484552-a38a71f3-0ca8-4b2f-943a-2d6317d6731a.png)  

**Pyspark Script**  
1.- Create Bucket:  
  ![image](https://user-images.githubusercontent.com/63972784/208486058-e811aa0b-848c-4b9d-986e-6ea21da7fefd.png)  
2.- Upload main.py file to the bucket.  

**Cloud Function Code**  
import argparse
import os
import re
from logging import exception
from google.cloud import dataproc_v1
from google.cloud import storage


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


def start_heavy_load(context):
    try:
        quickstart(project_id, region, cluster_name, pyspark_file)
        submit_job(project_id, region, cluster_name, pyspark_file)
        delete_cluster(project_id, region, cluster_name)
    except:
        print(
            "---------------INFO: AN ERROR HAPPENED, PLEASE CHECK LOGS TO HAVE MORE INFORMATION---------------------"
        )

