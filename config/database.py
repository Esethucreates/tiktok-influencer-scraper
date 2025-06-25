from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from dotenv import load_dotenv
import os

load_dotenv()

SQLALCHEMY_DATABASE_URI = os.getenv("POSTGRES_DB_URL")

engine = create_engine(SQLALCHEMY_DATABASE_URI)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    """
    Return session for database
    :return: SessionLocal
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class DatabaseSetup:
    def __init__(self):
        """
        Initialize database setup with connection URL
        """
        self.engine = create_engine(SQLALCHEMY_DATABASE_URI)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        self.Base = declarative_base()
