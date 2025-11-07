
# Transaction Microservice

A Flask-based microservice for managing transactions, using SQLite for local development. Supports Docker Compose and Minikube for easy local deployment.

## Features

- RESTful API endpoints for transaction management
- SQLite database integration using SQLAlchemy
- Docker Compose for local multi-service setup
- Kubernetes manifests for Minikube
- Postman collection for easy API testing
- OpenAPI (Swagger) documentation using Flasgger

## Setup

### Local Python (Dev)
1. Create a virtual environment:
    ```bash
    python -m venv venv
    .\venv\Scripts\activate  # Windows
...
    ```bash
    eval $(minikube docker-env)
    docker build -t transaction-microservice:latest .
    ```
2. Apply manifests:
    ```bash
    kubectl apply -f k8s/
    ```
3. Expose service:
    ```bash
    minikube service transaction-service
    ```

## API Endpoints

### Create Transaction

   To test the API endpoints, import the Postman collection or access the Swagger UI at `/` after deploying the application.

- **POST** `/transactions`
  - Example body:
     ```json
     {
...

### Get All Transactions
- **GET** `/transactions`

### Get Transaction by ID
- **GET** `/transactions/<txn_id>`

## Postman Collection

Import the file `transaction-microservice.postman_collection.json` into Postman to test all routes.

## Testing

Run tests using pytest:
```bash
pytest

## OpenAPI Documentation

Access the OpenAPI documentation at `/` to view and interact with the API endpoints.
```