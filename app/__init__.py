from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from os import path
import os
from dotenv import load_dotenv
from prometheus_flask_exporter import PrometheusMetrics
from prometheus_client import Counter, Histogram
import logging
from pythonjsonlogger import jsonlogger

# Load environment variables
load_dotenv()

# Initialize SQLAlchemy
db = SQLAlchemy()

def mask_pii(record):
    # Mask email/phone in logs
    if hasattr(record, 'email'):
        record.email = "***"
    if hasattr(record, 'phone'):
        record.phone = "***"
    return True

def create_app():
    app = Flask(__name__)
    
    # Configure SQLite database
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev')
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///transactions.db'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # Initialize database with app
    db.init_app(app)
    # Add Prometheus metrics
    metrics = PrometheusMetrics(app)
    # Custom business metrics

    app.transactions_total = Counter(
        'transactions_total', 'Total number of transactions', ['txn_type']
    )
    app.failed_transfers_total = Counter(
        'failed_transfers_total', 'Total failed transfer transactions'
    )
    app.balance_check_latency_ms = Histogram(
        'balance_check_latency_ms', 'Latency for balance check', buckets=(10, 50, 100, 250, 500, 1000, 2000)
    )
    # Register blueprints
    from .routes import main
    app.register_blueprint(main)
    
    # Create database if it doesn't exist
    with app.app_context():
        db.create_all()
    
    # Structured JSON logging
    handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter()
    handler.setFormatter(formatter)
    app.logger.handlers = [handler]
    app.logger.setLevel(logging.INFO)
    # Add filter for PII masking
    handler.addFilter(mask_pii)
    
    return app