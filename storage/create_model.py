from model import WindReport, TempReport, Base
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import inspect
from app import engine, app

Base.metadata.drop_all(engine) # Drop tables so we have a fresh start
Base.metadata.create_all(engine)
