from sqlalchemy.orm import DeclarativeBase, mapped_column
from sqlalchemy import Integer, String, DateTime, func

class Base(DeclarativeBase):
    pass

class WindReport(Base):
    __tablename__ = "wind_report"
    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id = mapped_column(String(50), nullable=False)
    device_id = mapped_column(String(50), nullable=False)
    timeStamp = mapped_column(DateTime, nullable=False)
    windspeed = mapped_column(Integer, nullable=False)
    date_created = mapped_column(DateTime,nullable=False, default=func.now())
    trace_id = mapped_column(String(50), nullable=False,index=True)

class TempReport(Base):
    __tablename__ = "temp_report"
    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id = mapped_column(String(50), nullable=False)
    device_id = mapped_column(String(50),nullable=False)
    timeStamp = mapped_column(DateTime,nullable=False)
    temperature = mapped_column(Integer,nullable=False)
    date_created = mapped_column(DateTime,nullable=False, default=func.now())
    trace_id = mapped_column(String(50), nullable=False,index=True)
    

