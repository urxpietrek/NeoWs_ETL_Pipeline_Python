from project.exctract.extractors import NeoWsExtractor
from project.utils import (
    add_days_to_date,
    process_file)

import sqlalchemy
from typing import Optional, Callable

class Pipeline:
    def __init__(self, api_key: str):
        self.extractor = NeoWsExtractor(api_key)
    
    def extract(self, start_date: str, end_date: Optional[str] = None):
        try:
            print(f"Starting data extraction from {start_date} to {end_date or 'end_date'}")
            self.extractor.extract(start_date, end_date)
            print("Data extraction complete")
        except Exception as error:
            print(f"Error during data extraction: {error}")
            raise
        
    def transform_and_load(self, session_factory: Callable , start_date: str, end_date: Optional[str] = None):
        try:
            from project.model import TargetData
        except ImportError:
            return

        if end_date is None:
            end_date = add_days_to_date(start_date)
        
        file_path = '_'.join(['NeoWs_json', start_date, end_date]) + '.json'
        records = process_file(file_path)

        if records:
            try:
                with session_factory() as s:
                    s.bulk_insert_mappings(TargetData, records)
                    s.commit()
                    print('Data inserted successfully')
            except sqlalchemy.exc.SQLAlchemyError as error:
                print(f'Error during data insertion: {error}')
                s.rollback()
            finally:
                s.close()
    
    def do_pipeline(self, session_factory, start_date: str, end_date: Optional[str] = None):
        self.extract(start_date, end_date)
        self.transform_and_load(session_factory, start_date, end_date)