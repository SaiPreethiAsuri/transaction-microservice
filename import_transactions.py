import csv
from datetime import datetime
from app import create_app, db
from app.models import Transaction, Idempotency
import os
import hashlib
import uuid
import argparse
import sys


def parse_date(date_str):
    """Convert date string to datetime object.

    CSV date format is 'DD-MM-YYYY HH:MM'. If parsing fails, returns None.
    """
    try:
        return datetime.strptime(date_str, '%d-%m-%Y %H:%M')
    except Exception:
        return None


def import_transactions(csv_file, force_recreate=False):
    """Import transactions from csv_file.

    Behavior:
      - If an Idempotency entry with the CSV reference exists, the mapped Transaction
        will be updated (idempotent update).
      - If not, a new Transaction is created and an Idempotency row is inserted.
      - By default this will not drop existing tables; pass force_recreate=True to
        drop & recreate all tables (destructive).
    """

    app = create_app()

    with app.app_context():
        if force_recreate:
            db.drop_all()
        db.create_all()

        if not csv_file:
            csv_file = os.getenv('CSV_FILE_PATH', 'transactions_1.csv')

        with open(csv_file, 'r', newline='', encoding=os.getenv('CSV_ENCODING', 'utf-8')) as file:
            csv_reader = csv.DictReader(file)

            # Single loop over csv_reader (removed nested loop)
            for row in csv_reader:
                # compute request_hash (use reference when available for stability)
                raw_ref = (row.get('reference') or '').strip()
                if raw_ref:
                    request_hash = raw_ref
                else:
                    # Use a stable serialization for hashing
                    request_hash = hashlib.sha256(repr(sorted(row.items())).encode('utf-8')).hexdigest()

                # Try to find an existing idempotency mapping by request_hash
                idem = Idempotency.query.filter_by(request_hash=request_hash).first()

                provided_txn_id = int(row.get('txn_id')) if row.get('txn_id') else None

                # Determine correlation id: reuse existing mapping key if present, else generate new
                if idem and getattr(idem, 'key', None):
                    corr_id = idem.key
                else:
                    corr_id = str(uuid.uuid4())

                # parse amount and created_dt
                try:
                    amount = float(row.get('amount') or 0)
                except ValueError:
                    amount = 0.0

                created_dt = parse_date(row.get('created_at') or '')

                # If idempotency exists, attempt to find the mapped transaction to update
                existing = None
                if idar := idem:
                    # Prefer explicit txn_id if provided
                    if provided_txn_id:
                        existing = Transaction.query.get(provided_txn_id)
                    # Otherwise try to find by reference (if reference was used as request_hash)
                    if not existing and raw_ref:
                        existing = Transaction.query.filter_by(reference=raw_ref).first()

                # If provided_txn_id is given and not found yet, try fetching by that id (safe path)
                if not existing and provided_txn_id:
                    existing = Transaction.query.get(provided_txn_id)

                if existing:
                    # Update existing transaction
                    existing.account_id = int(row.get('account_id')) if row.get('account_id') else existing.account_id
                    existing.counterparty_id = row.get('counterparty_id') or existing.counterparty_id
                    existing.amount = amount
                    existing.txn_type = row.get('txn_type') or existing.txn_type
                    existing.reference = raw_ref or existing.reference
                    if created_dt:
                        existing.created_dt = created_dt
                    existing.failure_status = row.get('failure_status') or existing.failure_status
                    existing.correlation_id = corr_id
                    try:
                        db.session.commit()
                        print(f"Updated transaction txn_id={existing.txn_id or existing.id}")
                    except Exception as e:
                        print(f"Error updating transaction for txn_id {provided_txn_id}: {e}")
                        db.session.rollback()
                    new_transaction = existing
                else:
                    # Insert new transaction
                    new_transaction = Transaction(
                        txn_id=provided_txn_id,
                        account_id=int(row.get('account_id')) if row.get('account_id') else None,
                        counterparty_id=row.get('counterparty_id'),
                        amount=amount,
                        txn_type=row.get('txn_type'),
                        reference=raw_ref,
                        created_dt=created_dt or datetime.utcnow(),
                        failure_status=row.get('failure_status'),
                        correlation_id=corr_id
                    )
                    db.session.add(new_transaction)
                    try:
                        db.session.commit()
                        print(f"Inserted transaction reference={raw_ref} txn_id={new_transaction.txn_id or new_transaction.id}")
                    except Exception as e:
                        print(f"Error inserting transaction for reference {raw_ref}: {e}")
                        db.session.rollback()

                # Create idempotency mapping if none existed
                if new_transaction and not idem:
                    mapping = Idempotency(key=corr_id, request_hash=request_hash)
                    db.session.add(mapping)
                    try:
                        db.session.commit()
                    except Exception as e:
                        print(f"Error creating idempotency mapping for correlation_id {corr_id}: {e}")
                        db.session.rollback()


# Add a CLI entrypoint so running the script actually executes the import
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import transactions from CSV.")
    parser.add_argument("csv_file", nargs="?", help="Path to CSV file (defaults to env CSV_FILE_PATH or transactions_1.csv)")
    parser.add_argument("--force", action="store_true", help="Drop and recreate tables before importing")
    args = parser.parse_args()

    try:
        import_transactions(args.csv_file, force_recreate=args.force)
    except FileNotFoundError:
        print(f"CSV file not found: {args.csv_file or os.getenv('CSV_FILE_PATH','transactions_1.csv')}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"Unhandled error: {e}", file=sys.stderr)
        sys.exit(1)