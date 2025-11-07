from . import db
from datetime import datetime

class Transaction(db.Model):
    # Use txn_id as the primary key to match the requested table structure
    txn_id = db.Column(db.Integer, primary_key=True)
    account_id = db.Column(db.Integer, nullable=True, index=True)
    counterparty_id = db.Column(db.String(128), nullable=True)
    amount = db.Column(db.Float, nullable=False)
    txn_type = db.Column(db.String(50), nullable=False)
    reference = db.Column(db.String(128), unique=True, nullable=True, index=True)
    created_dt = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    failure_status = db.Column(db.String(200), nullable=True)
    correlation_id = db.Column(db.String(128), nullable=True, index=True)

    def __repr__(self):
        return f'<Transaction {self.txn_id}: {self.txn_type} {self.amount}>'


class Idempotency(db.Model):
        """Idempotency mapping keyed by transaction.correlation_id.

        Columns:
            - key: foreign key to transaction.correlation_id
            - request_hash: hash or identifier of the incoming request
            - created_at: when the mapping was recorded

        We use a composite primary key (key, request_hash) to ensure uniqueness
        of a particular request for a specific correlation id.
        """
        __tablename__ = 'idempotency'

        key = db.Column(db.String(128), db.ForeignKey('transaction.correlation_id'), primary_key=True)
        request_hash = db.Column(db.String(128), primary_key=True)
        created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

        # optional relationship to the transaction (correlation_id -> transaction)
        transaction = db.relationship('Transaction', primaryjoin="Transaction.correlation_id==Idempotency.key", backref='idempotency_mappings', single_parent=True)