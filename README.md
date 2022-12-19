# dataproc-cloud-function
Script to create a cluster in dataproc, submit a job and delete cluster  
Steps to create and execute Cloud Function:  
1.- Create cloud function:  
  ![image](https://user-images.githubusercontent.com/63972784/208485173-cf096e17-2074-4a48-8e2f-e1d790801500.png)
  Set the "Timeout" to 540s, this due to the time gcp takes to create the cluster, depending on your script will also increase or decrease the time of execution and finally the time it takes to delete the cluster:  
  ![image](https://user-images.githubusercontent.com/63972784/208483961-4841fc6b-32ae-4d26-9a52-e33b021d6415.png)
  Choose Python 3.7 as Runtime, paste main.py code and make sure start_heavy_load is set as Entry Point:  
  ![image](https://user-images.githubusercontent.com/63972784/208484480-5cb1571c-8b44-4164-ad0d-fb1f57d124c6.png)
  Add google-cloud-dataproc as librarie in the requirements.txt file:  
  ![image](https://user-images.githubusercontent.com/63972784/208484552-a38a71f3-0ca8-4b2f-943a-2d6317d6731a.png)

