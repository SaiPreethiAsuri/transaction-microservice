
# Transaction Microservice

A Flask-based microservice for managing transactions, using SQLite for local development. Supports Docker Compose and Minikube for easy local deployment.

## Features

- RESTful API endpoints for transaction management
- SQLite database integration using SQLAlchemy
- Docker Compose for local multi-service setup
- Kubernetes manifests for Minikube
- Postman collection for easy API testing

## Setup

### Local Python (Dev)
1. Create a virtual environment:
    ```bash
    python -m venv venv
    .\venv\Scripts\activate  # Windows
    source venv/bin/activate  # Linux/Mac
    ```
2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3. (Optional) Create `.env` file:
    ```bash
    SECRET_KEY=your-secret-key
    ```

### Docker Compose (Recommended for Local)
1. Build and start the service:
    ```bash
    docker compose up --build -d
    ```
    - SQLite DB is stored in the `instance/transactions.db` file (mounted as a volume).

### Minikube (Kubernetes Local)
1. Build Docker image and load into Minikube:
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
- **POST** `/transactions`
  - Example body:
     ```json
     {
        "txn_id": 1,
        "account_id": 123,
        "counterparty_id": "abc",
        "amount": 100.0,
        "txn_type": "transfer",
        "reference": "REF123",
        "created_dt": "2025-11-06T12:00:00",
        "correlation_id": "uuid-example"
     }
     ```

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
```