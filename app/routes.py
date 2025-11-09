from flask import Blueprint, request, jsonify, current_app
from .models import Transaction, Idempotency
from . import db
import requests
import hashlib
import json
import time
import os

main = Blueprint('main', __name__)

# Route to create transaction
@main.route('/transactions', methods=['POST'])
def create_transaction():
    """
    Create a new transaction.
    This endpoint creates a new transaction. For 'transfer' type, it creates two transaction records (a withdrawal and a deposit).
    It supports idempotency via a `correlation_id` in the request body.
    ---
    tags:
      - Transactions
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          required:
            - amount
            - txn_type
          properties:
            account_id:
              type: integer
              description: The ID of the account initiating the transaction.
            counterparty_id:
              type: string
              description: The ID of the counterparty account (required for transfers).
            amount:
              type: number
              format: float
              description: The transaction amount.
            txn_type:
              type: string
              description: The type of transaction (e.g., 'deposit', 'withdrawal', 'transfer').
            reference:
              type: string
              description: A client-provided reference for the transaction.
            correlation_id:
              type: string
              description: A unique ID for ensuring idempotency of the request.
    responses:
      201:
        description: Transaction created successfully.
        schema:
          type: object
          properties:
            message:
              type: string
            txn_id:
              type: integer
              description: The ID of the created transaction (for non-transfers).
            withdrawal_txn_id:
              type: integer
              description: The ID of the withdrawal part of a transfer.
            deposit_txn_id:
              type: integer
              description: The ID of the deposit part of a transfer.
      400:
        description: Bad Request - Missing required fields, invalid amount, or other validation errors.
        schema:
          type: object
          properties:
            error:
              type: string
      409:
        description: Conflict - Duplicate transaction based on correlation_id and request payload.
        schema:
          type: object
          properties:
            message:
              type: string
            original_txn_id:
              type: integer
      500:
        description: Internal Server Error.
        schema:
          type: object
          properties:
            error: 
              type: string
      502:
        description: Bad Gateway - Error communicating with a downstream service.
        schema:
          type: object
          properties:
            error:
              type: string
    """
    # Ensure request data is read before using it
    data = request.get_json() or {}
    start_time = time.time()

    from datetime import datetime, timedelta
    DAILY_LIMIT = 200000

    # Fetch external service URLs from environment variables
    ACCOUNTS_SERVICE_URL = os.getenv("ACCOUNTS_SERVICE_URL", "http://accounts-microservice/accounts/check")
    NOTIFICATION_SERVICE_URL = os.getenv("NOTIFICATION_SERVICE_URL", "http://notification-microservice/notify")

    # Minimal required fields
    if 'amount' not in data or 'txn_type' not in data:
        return jsonify({'error': 'Missing required fields: amount and txn_type'}), 400

    # Enforce daily transaction limit per account (apply when account_id present)
    try:
        amount = float(data.get('amount') or 0)
    except Exception:
        return jsonify({'error': 'Invalid amount'}), 400

    account_id = data.get('account_id')
    if account_id:
        try:
            today = datetime.utcnow().date()
            start_of_day = datetime(today.year, today.month, today.day)
            end_of_day = start_of_day + timedelta(days=1)
            total_today = db.session.query(db.func.coalesce(db.func.sum(Transaction.amount), 0)).filter(
                Transaction.account_id == int(account_id),
                Transaction.created_dt >= start_of_day,
                Transaction.created_dt < end_of_day
            ).scalar() or 0
            if total_today + amount > DAILY_LIMIT:
                return jsonify({'error': f'Daily transaction limit of {DAILY_LIMIT} exceeded for account {account_id}'}), 400
        except Exception as e:
            # If query fails, return an error rather than proceeding silently
            return jsonify({'error': f'Error checking daily limit: {str(e)}'}), 500

    # Generate request hash for idempotency check
    request_data = {k: v for k, v in sorted(data.items()) if k != 'correlation_id'}
    request_hash = hashlib.sha256(json.dumps(request_data).encode()).hexdigest()

    # Check for duplicate transaction using Idempotency table
    if data.get('correlation_id'):
        existing_idempotency = Idempotency.query.filter_by(
            key=data['correlation_id'],
            request_hash=request_hash
        ).first()
        
        if existing_idempotency:
            # Duplicate request — return conflict and reference existing mapping if available
            try:
                original_txn_id = getattr(existing_idempotency, 'transaction', None)
                orig_id = original_txn_id.txn_id if original_txn_id else None
            except Exception:
                orig_id = None
            return jsonify({
                'message': 'Duplicate transaction',
                'original_txn_id': orig_id
            }), 409  # Conflict status code

    failure_status = None
    txn_type = data.get('txn_type', 'unknown')

    # For transfer transactions, validate and create two separate records (withdrawal + deposit)
    if txn_type == 'transfer':
        account_id = data.get('account_id')
        counterparty_id = data.get('counterparty_id')
        try:
            amount = float(data['amount'])
        except Exception:
            return jsonify({'error': 'Invalid amount for transfer'}), 400

        if not account_id or not counterparty_id:
            return jsonify({'error': 'account_id and counterparty_id required for transfer'}), 400

        # Use ACCOUNTS_SERVICE_URL from env
        accounts_check_url = f"{ACCOUNTS_SERVICE_URL.rstrip('/')}/check"

        # Validate account/counterparty status and balance (no overdraft allowed)
        try:
            with current_app.balance_check_latency_ms.time():
                resp = requests.post(
                    accounts_check_url,
                    json={'account_id': account_id, 'counterparty_id': counterparty_id},
                    timeout=5
                )

            if resp.status_code != 200:
                return jsonify({'error': 'Failed to fetch account or counterparty info'}), 502

            resp_data = resp.json()
            acc_data = resp_data.get('account', {})
            cp_data = resp_data.get('counterparty', {})

            # Validate statuses
            if acc_data.get('status') == 'FROZEN' or cp_data.get('status') == 'FROZEN':
                return jsonify({'error': 'Account or counterparty is frozen'}), 400
            if acc_data.get('status') != 'ACTIVE' or cp_data.get('status') != 'ACTIVE':
                return jsonify({'error': 'Account or counterparty is not active'}), 400

            # Prevent overdraft
            sender_balance = float(acc_data.get('balance', 0))
            if sender_balance < amount:
                return jsonify({'error': 'Insufficient balance in account; overdraft not allowed'}), 400

        except requests.exceptions.RequestException as e:
            return jsonify({'error': f'Error contacting accounts service: {str(e)}'}), 502
        except Exception as e:
            return jsonify({'error': f'Unexpected error validating accounts: {str(e)}'}), 500

        # Create withdrawal and deposit records atomically
        try:
            # withdrawal: from sender account (store negative amount to reflect debit)
            withdrawal = Transaction(
                account_id=int(account_id),
                counterparty_id=counterparty_id,
                amount=-abs(amount),
                txn_type='withdrawal',
                reference=data.get('reference'),
                created_dt=data.get('created_dt'),
                failure_status=None,
                correlation_id=data.get('correlation_id')
            )

            # deposit: to counterparty account (positive amount)
            deposit = Transaction(
                account_id=int(counterparty_id),
                counterparty_id=account_id,
                amount=abs(amount),
                txn_type='deposit',
                reference=data.get('reference'),
                created_dt=data.get('created_dt'),
                failure_status=None,
                correlation_id=data.get('correlation_id')
            )

            db.session.add(withdrawal)
            db.session.add(deposit)

            # Create idempotency record if correlation_id exists
            if data.get('correlation_id'):
                idempotency = Idempotency(
                    key=data['correlation_id'],
                    request_hash=request_hash
                )
                db.session.add(idempotency)
            db.session.commit()

            # Update metrics for both records
            current_app.transactions_total.labels(txn_type='withdrawal').inc()
            current_app.transactions_total.labels(txn_type='deposit').inc()

            # Update the accounts service 
            try:
                update_payload = {
                  "account_id": account_id,
                  "counterparty_id": counterparty_id,
                  "amount": amount,
                  "txn_type": txn_type
                }
                account_update_url= f"{ACCOUNTS_SERVICE_URL.rstrip('/')}/update-balance"
                
                balance_resp= requests.post(
                  account_update_url,
                  json=update_payload,
                  timeout=5
                )
                if balance_resp.status_code!=200:
                  return jsonify({
                    "error":"Balance update failed",
                    "details":balance_resp.text
                  }), 502
            except Exception as e:
              return jsonify({
                "error":f"Error contacting account service: {str(e)}"
              }), 502

            # Notify external notification service for both transactions
            for tx in [withdrawal, deposit]:
                try:
                    notification_payload = {
                        "txn_id": tx.txn_id or tx.id,
                        "reference": tx.reference,
                        "status": "success"
                    }
                    requests.post(NOTIFICATION_SERVICE_URL, json=notification_payload, timeout=3)
                except Exception as notify_err:
                    current_app.logger.warning(f"Notification service call failed for txn_id {tx.txn_id or tx.id}: {notify_err}")

            return jsonify({
                'message': 'Transfer completed',
                'withdrawal_txn_id': withdrawal.txn_id or withdrawal.id,
                'deposit_txn_id': deposit.txn_id or deposit.id
            }), 201

        except Exception as e:
            db.session.rollback()
            return jsonify({'error': f'Error creating transfer transactions: {str(e)}'}), 500

    # Create transaction (non-transfer)
    try:
        txn = Transaction(
            account_id=int(data['account_id']) if data.get('account_id') else None,
            counterparty_id=data.get('counterparty_id'),
            amount=float(data['amount']),
            txn_type=data.get('txn_type'),
            reference=data.get('reference'),
            created_dt=data.get('created_dt'),
            failure_status=failure_status,
            correlation_id=data.get('correlation_id')
        )

        # Create idempotency record if correlation_id exists
        if data.get('correlation_id'):
            idempotency = Idempotency(
                key=data['correlation_id'],
                request_hash=request_hash
            )
            db.session.add(idempotency)

        db.session.add(txn)
        db.session.commit()
        
        # Business metric: increment total transactions
        current_app.transactions_total.labels(txn_type=txn.txn_type or 'unknown').inc()
        
        #Update Account Service Balance
        try:
          update_payload = {
                  "account_id": account_id,
                  "amount": amount,
                  "txn_type": txn_type
          }
          account_update_url= f"{ACCOUNTS_SERVICE_URL.rstrip('/')}/update-balance"
                
          balance_resp= requests.post(
            account_update_url,
            json=update_payload,
            timeout=5
          )
          if balance_resp.status_code!=200:
            return jsonify({
                    "error":"Balance update failed",
                    "details":balance_resp.text
                  }), 502
        except Exception as e:
            db.session.rollback()
            return jsonify({'error': f'Error creating transfer transactions: {str(e)}'}), 500
          
        

        # Notify external notification service
        try:
            notification_payload = {
                "txn_id": txn.txn_id,
                "reference": txn.reference,
                "status": "failed" if failure_status else "success"
            }
            requests.post(NOTIFICATION_SERVICE_URL, json=notification_payload, timeout=3)
        except Exception as notify_err:
            current_app.logger.warning(f"Notification service call failed for txn_id {txn.txn_id}: {notify_err}")

        if failure_status:
            if txn.txn_type == 'transfer':
                current_app.failed_transfers_total.inc()
            return jsonify({'message': 'Transaction failed', 'txn_id': txn.txn_id, 'failure_status': failure_status}), 201
        return jsonify({'message': 'Transaction created successfully', 'txn_id': txn.txn_id}), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({'error': str(e)}), 500


# Route to fetch all transactions
@main.route('/transactions', methods=['GET'])
def get_transactions():
    """
    Get all transactions.
    This endpoint retrieves a list of all transactions in the system.
    ---
    tags:
      - Transactions
    responses:
      200:
        description: A list of transactions.
        schema:
          type: array
          items:
            $ref: '#/definitions/Transaction'
      500:
        description: Internal Server Error.
        schema:
          type: object
          properties:
            error:
              type: string
    definitions:
      Transaction:
        type: object
        properties:
          # Define transaction properties here for schema
          txn_id: { type: integer }
          account_id: { type: integer }
          counterparty_id: { type: string }
          amount: { type: number, format: float }
          txn_type: { type: string }
          reference: { type: string }
          created_dt: { type: string, format: 'date-time' }
          failure_status: { type: string }
          correlation_id: { type: string }
    """
    try:
        transactions = Transaction.query.all()
        return jsonify([{
            'txn_id': t.txn_id,
            'account_id': t.account_id,
            'counterparty_id': t.counterparty_id,
            'amount': t.amount,
            'txn_type': t.txn_type,
            'reference': t.reference,
            'created_dt': t.created_dt.isoformat() if t.created_dt else None,
            'failure_status': t.failure_status,
            'correlation_id': t.correlation_id
        } for t in transactions]), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Route to fetch specific transaction by id
@main.route('/transactions/<int:txn_id>', methods=['GET'])
def get_transaction(txn_id):
    """
    Get a specific transaction by its ID.
    This endpoint retrieves the details of a single transaction.
    ---
    tags:
      - Transactions
    parameters:
      - name: txn_id
        in: path
        type: integer
        required: true
        description: The unique ID of the transaction.
    responses:
      200:
        description: The transaction details.
        schema:
          $ref: '#/definitions/Transaction'
      404:
        description: Transaction not found.
      500:
        description: Internal Server Error.
        schema:
          type: object
          properties:
            error:
              type: string
    """
    try:
        transaction = Transaction.query.get_or_404(txn_id)
        return jsonify({
            'txn_id': transaction.txn_id,
            'account_id': transaction.account_id,
            'counterparty_id': transaction.counterparty_id,
            'amount': transaction.amount,
            'txn_type': transaction.txn_type,
            'reference': transaction.reference,
            'created_dt': transaction.created_dt.isoformat() if transaction.created_dt else None,
            'failure_status': transaction.failure_status,
            'correlation_id': transaction.correlation_id
        }), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500