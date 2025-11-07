from flask import Blueprint, request, jsonify, current_app
from .models import Transaction, Idempotency
from . import db
import requests
import hashlib
import json
import time

main = Blueprint('main', __name__)

# Route to create transaction
@main.route('/transactions', methods=['POST'])
def create_transaction():
    # Ensure request data is read before using it
    data = request.get_json() or {}
    start_time = time.time()

    from datetime import datetime, timedelta
    DAILY_LIMIT = 200000

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

        accounts_service_url = "http://accounts-microservice/accounts/check"

        # Validate account/counterparty status and balance (no overdraft allowed)
        try:
            with current_app.balance_check_latency_ms.time():
                resp = requests.post(
                    accounts_service_url,
                    json={'account_id': account_id, 'counterparty_id': counterparty_id},
                    timeout=5
                )

            if resp.status_code != 200:
                return jsonify({'error': 'Failed to fetch account or counterparty info'}), 502

            resp_data = resp.json()
            acc_data = resp_data.get('account', {})
            cp_data = resp_data.get('counterparty', {})

            # Validate statuses
            if acc_data.get('status') == 'frozen' or cp_data.get('status') == 'frozen':
                return jsonify({'error': 'Account or counterparty is frozen'}), 400
            if acc_data.get('status') != 'active' or cp_data.get('status') != 'active':
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