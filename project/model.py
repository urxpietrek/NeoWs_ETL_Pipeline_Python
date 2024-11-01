from sqlalchemy import Column, Integer
from sqlalchemy.orm import DeclarativeBase
from project.utils import get_mysql_engine

from config import settings
engine = get_mysql_engine(**settings.database)

class BaseModel(DeclarativeBase):
    __abstract__ = True
    __table_args__ = {'autoload_with' : engine}
    
class TargetData(BaseModel):
    __tablename__ = 'asteroids_details'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    
TableStaging = TargetData.__table__.to_metadata(
    BaseModel.metadata, name=f'{TargetData.__tablename__}_temp'
)

TableStaging._prefixes = ['TEMPORARY']

class StageData(BaseModel):
    __table__ = TableStaging
    
    def __repr__(self):
        return f"{StageData.__table__.columns}"
    
    def as_dict(self):
        return self.__dict__