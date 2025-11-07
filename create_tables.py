from app import db
from app import models

def create_all_tables():
    from app import create_app
    app = create_app()
    with app.app_context():
        db.create_all()
        print("All tables created successfully.")

if __name__ == "__main__":
    create_all_tables()
