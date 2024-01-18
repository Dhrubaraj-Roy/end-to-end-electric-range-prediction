 python3 -m venv range
 source range/bin/activate
 data path  = /home/dhruba/gigs_project/project_b/FacilityFinder/data/Facilities_Database.csv
 prefect deployment  build flow.py:my_flow -n range_prediction 
  prefect deployment apply my_flow-deployment.yaml 
  prefect agent start -q 'default'

 {
  "Postal_Code": 98103,
  "Model_Year": 2020,
  "Legislative_District": 43
}

<!-- uvicorn app:app --reload --># end-to-end-electric-range-prediction


Docker file 
the docker image name is range to create the range image follow this command 
docker build -t range .


After the build is successful, you can run a container based on the image with:
docker run -p 80:80 range
This command maps port 80 on your local machine to port 80 in the Docker container. Adjust the ports as needed.

Now, your Docker container should be running, and you can access the application at http://localhost:80.



Kubernetes

Great! To deploy your application on Azure Kubernetes Service (AKS), follow these steps:

1. **Create an Azure Kubernetes Service Cluster:**
   - Open the Azure portal and navigate to the Azure Kubernetes Service.
   - Click on "Add" to create a new AKS cluster.
   - Follow the wizard to configure your AKS cluster. Provide details such as subscription, resource group, cluster name, region, etc.
   - Choose the appropriate settings for your use case, and then click "Review + create" to create the AKS cluster.

2. **Configure kubectl:**
   - After the AKS cluster is created, you need to configure `kubectl` to connect to the cluster.
   - Use the following command to get the credentials for the AKS cluster:
     ```bash
     az aks get-credentials --resource-group <your-resource-group> --name <your-aks-cluster-name>
     ```
     Replace `<your-resource-group>` and `<your-aks-cluster-name>` with your actual resource group and AKS cluster name.

3. **Build and Push Docker Image:**
   - Build your Docker image if you haven't already:
     ```bash
     docker build -t your_image_name .
     ```
   - Tag your Docker image with the Azure Container Registry (ACR) login server. Replace `<your-acr-name>` with your ACR name:
     ```bash
     docker tag your_image_name <your-acr-name>.azurecr.io/your_image_name:v1
     ```
   - Log in to your ACR:
     ```bash
     docker login <your-acr-name>.azurecr.io
     ```
   - Push the Docker image to the ACR:
     ```bash
     docker push <your-acr-name>.azurecr.io/your_image_name:v1
     ```

4. **Deploy to AKS:**
   - Create a Kubernetes deployment file (e.g., `deployment.yaml`) to deploy your application. Here's a simple example:
     ```yaml
     apiVersion: apps/v1
     kind: Deployment
     metadata:
       name: your-app
     spec:
       replicas: 3
       selector:
         matchLabels:
           app: your-app
       template:
         metadata:
           labels:
             app: your-app
         spec:
           containers:
           - name: your-app
             image: <your-acr-name>.azurecr.io/your_image_name:v1
             ports:
             - containerPort: 80
     ```
   - Apply the deployment to your AKS cluster:
     ```bash
     kubectl apply -f deployment.yaml
     ```

5. **Expose Your Application:**
   - Create a Kubernetes service to expose your application:
     ```bash
     kubectl expose deployment your-app --type=LoadBalancer --name=your-app-service
     ```

6. **Access Your Application:**
   - Retrieve the external IP address assigned to your service:
     ```bash
     kubectl get services your-app-service
     ```
   - Access your application using the external IP address.

Now, your application should be running on Azure Kubernetes Service. Adjust the configurations as needed for your specific requirements.