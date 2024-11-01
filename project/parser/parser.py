from abc import ABC, abstractmethod
from project.model import StageData

from datetime import datetime

class Parser(ABC):
    def __init__(self, records):
        self.check_and_set_records(records)
        
    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)
        
    def check_and_set_records(self, records):
        if hasattr(records, '__iter__') and not isinstance(records, dict):
            self._records = records
        elif isinstance(records, dict):
            self._records = iter([records])
        elif isinstance(records, (list, tuple)):
            self._records = iter(records)
        else:
            raise ValueError('The type of records is unknown.')
    
    @property
    def records(self):
        return self._records   
        
    @records.setter
    def records(self, value):
        self.check_and_set_records(value)
    
    def __iter__(self):
        return self
    
    def __next__(self):
        record = next(self._records)
        parsed_record = self.parse(record)
        return parsed_record
        
    @abstractmethod
    def parse(self):
        pass

class AsteroidParser(Parser):
    
    def parse(self, record):
        data = StageData()
        
        data.asteroid_id = record['id']
        data.neo_reference_id = record['neo_reference_id']
        data.absolute_magnitude = record['absolute_magnitude_h']
        data.estimated_diameter_km_max = record['estimated_diameter']['kilometers']['estimated_diameter_max']
        data.estimated_diameter_km_min = record['estimated_diameter']['kilometers']['estimated_diameter_min']
        data.isHazardous = record['is_potentially_hazardous_asteroid']
        data.close_approach_date = record['close_approach_data'][0]['close_approach_date']  # Adjusting for a list
        data.miss_distance_km = record['close_approach_data'][0]['miss_distance']['kilometers']  # Adjusting for a list
        data.uploaded_date = datetime.now()  # You can set this to the current date or any other value if needed
        return data.as_dict()

class Asteroid:
    def __init__(self, asteroid_id, neo_reference_id, absolute_magnitude,
                 estimated_diameter_km_max, estimated_diameter_km_min,
                 isHazardous, close_approach_date, miss_distance_km,
                 uploaded_date):
        self.asteroid_id = asteroid_id
        self.neo_reference_id = neo_reference_id
        self.absolute_magnitude = absolute_magnitude
        self.estimated_diameter_km_max = estimated_diameter_km_max
        self.estimated_diameter_km_min = estimated_diameter_km_min
        self.isHazardous = isHazardous
        self.close_approach_date = close_approach_date
        self.miss_distance_km = miss_distance_km
        self.uploaded_date = uploaded_date

    def __repr__(self):
        return (f"<Asteroid(asteroid_id={self.asteroid_id}, "
                f"neo_reference_id={self.neo_reference_id}, "
                f"absolute_magnitude={self.absolute_magnitude}, "
                f"estimated_diameter_km_max={self.estimated_diameter_km_max}, "
                f"estimated_diameter_km_min={self.estimated_diameter_km_min}, "
                f"isHazardous={self.isHazardous}, "
                f"close_approach_date={self.close_approach_date}, "
                f"miss_distance_km={self.miss_distance_km}, "
                f"uploaded_date={self.uploaded_date})>")

