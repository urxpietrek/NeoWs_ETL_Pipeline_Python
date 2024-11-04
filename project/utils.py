from sqlalchemy import create_engine, Engine, MetaData
from datetime import datetime, timedelta
from typing import Union

import requests
import logging
import json
import os
import re

JSON_FILE_PATH = os.path.abspath('./data/json_files')

logging.basicConfig(filename = 'sample.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s - %(message)s',
                    datefmt='%H:%M:%S')

logger = logging.getLogger(__name__)

def get_mysql_engine(host: str, user: str, password: str, port: int, database: str) -> Engine:
    """
        Function returns mysql engine with a given parameters to uri.
    """
    connection_uri = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'
    return create_engine(connection_uri)
    
def create_filename(start_date: str, end_date: str | None = None) -> str:
    """
        Create name based on dates.
    """
    if end_date is None:
        end_date = (
            datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=7)
            )\
            .strftime('%Y-%m-%d')
                    
    return '_'.join(['NeoWs_json', start_date, end_date]) + '.json'

def save_to_json(filename: str, data: requests.Response) -> tuple[bool, str]:
    if not os.path.exists(JSON_FILE_PATH):
        os.makedirs(JSON_FILE_PATH)
        
    try:
        path = os.path.join(JSON_FILE_PATH, filename)
        with open(path, mode = 'w') as json_file:
            json.dump(data, fp = json_file, indent=4)
        
    except IOError as e:
        return False, f"Error saving data to JSON: {e}"
    
    return True, path

def check_and_set_date_format(date: Union[str, datetime]) -> str:
    # Reformat datetime to str
    if isinstance(date, datetime):
        return  date.strftime('%Y-%m-%d')
        
    # Change str format to `YYYY-MM-DD`
    if isinstance(date, str):
        r = re.compile(r'^\d{4}-\d{2}-\d{2}$')
        if  r.match(date) is not None:
            return date
        
    raise ValueError('The given date format is wrong. Set date with `yyyy-mm-dd`')

def read_json_file(filepath: str):
    if not os.path.exists(filepath):
        raise IOError(f'Given path does not exists {filepath}.')
    
    data = None
    try:
        with open(filepath, mode='r') as json_file:
            data=json.load(fp=json_file)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except IOError as e:
        print(f"Error reading file: {e}")   
    
    return data

def get_available_files():
    for _, _, files in os.walk(JSON_FILE_PATH):
        print(*files)
    
def extract_data_from_json(filename: str):
    """
        Function which extract data from json_file.
    """
    
    filepath = os.path.join(JSON_FILE_PATH, filename)
    data = read_json_file(filepath)
    
    if data is None:
        return None, None
    
    datasets = data.get('near_earth_objects', {})
    if not datasets:
        return None, None
    
    dates, details = list(datasets.keys()), list(datasets.values())
    
    return dates, details

def add_days_to_date(date: str, days: int = 7):
    fmt = '%Y-%m-%d'
    formatted_date = datetime.strptime(date, fmt)    
    
    return (formatted_date + timedelta(days=days)).strftime(fmt)

def process_file(filename: str) -> list[list]:
    """
        Functions proccess the file with a given name located in data folder.
        
        :param filename: name of json file.
        
        :return: nested list with astroid details per date.
    """
    from project.parser.parser import AsteroidParser

    dates, record_sets = extract_data_from_json(filename)
    if not (dates or record_sets):
        logger.warning(f'No data found in file `{filename}`')
        return []

    aggregated_records = []
    parser = AsteroidParser([])

    for record_set in record_sets:
        for record in record_set:
            try:
                parser.records = record
                parsed_records = [parsed_record for parsed_record in parser]
                aggregated_records.extend(parsed_records)
            except Exception as e:
                logger.error(f'Failed to parse record {record}: {e}')
                continue 

    return aggregated_records