import sqlalchemy
import sqlalchemy.exc
from sqlalchemy.orm import sessionmaker
import pymysql as db

from config import settings
from project.utils import (
    extract_data_from_json,
    get_available_files,
    get_mysql_engine,
    add_days_to_date
)

from project.database import (
    database_exists,
    table_exists,
    create_database
)

from project.utils import logger

import constants.queries as const

from project.arguments import parse_arguments
from project.exctract.extractors import NeoWsExtractor

API_KEY = settings['API_KEY']

def get_session(engine: sqlalchemy.Engine):
    """
        Function which returns session with binded engine
    """
    return sessionmaker(bind=engine)

    
SessionFactory = get_session(get_mysql_engine(**settings.database))

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
             
                    
def create_asteroids_table() -> bool:
    """
        Function creates asteroid table in a given database parameters.
    """
    engine: sqlalchemy.Engine = get_mysql_engine(**settings.database)
    table_name = 'asteroids_details'
    db_name = settings.database['database']
    try:
        # Firstly create database
        with db.connect(**settings.database) as connection:
            if not database_exists(connection, db_name):
                create_database(settings.database)
            else:
                logger.info(f'Database `{db_name}` already exists. Skip creating.')
        
        if not table_exists(engine, table_name):
            with engine.connect() as connection:
                with connection.begin() as trans:
                    # Then create table `asteroid_details`
                    connection.execute(sqlalchemy.text(
                                        const.CREATE_ASTEROIDS_OBSERVATIONS_TABLE)
                                        )
                    trans.commit()
                    logger.info(f'Table `{table_name}` created succesfully.')
                    return True
        else:
            logger.info(f'Table `{table_name}` already exists. Skip creating.')
                
    except sqlalchemy.exc.SQLAlchemyError as e:
        print(f"Database error: {e}")
        if 'trans' in locals():
            trans.rollback()
        return False
    finally:
        engine.dispose()

def simple_extract():
    """
    Function for simple extraction from NeoWs API based on provided arguments.
    """
    start_date, end_date = args.extract.split(' ') if len(args.extract.split(' ')) > 1 else (args.extract, None)
    extractor = NeoWsExtractor(API_KEY)
    extraction_result = extractor.extract(start_date, end_date)
    print(extraction_result)
    
def simple_read_file():
    """
    Function to read and process a file, inserting its content into the database.
    """
    try:
        from project.model import TargetData
    except ImportError:
        return

    file_path = args.read_file
    records = process_file(file_path)

    if records:
        try:
            with SessionFactory() as session:
                session.bulk_insert_mappings(TargetData, records)
                session.commit()
                print('Data inserted successfully')
        except sqlalchemy.exc.SQLAlchemyError as error:
            print(f'Error during data insertion: {error}')
            session.rollback()
        finally:
            session.close()
            
def make_pipeline():
    start_date, end_date = args.pipeline.split(' ') if len(args.pipeline.split(' ')) > 1 else (args.pipeline, None)
    extractor = NeoWsExtractor(API_KEY)
    extraction_result = extractor.extract(start_date, end_date)
    
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
            with SessionFactory() as session:
                session.bulk_insert_mappings(TargetData, records)
                session.commit()
                print('Data inserted successfully')
        except sqlalchemy.exc.SQLAlchemyError as error:
            print(f'Error during data insertion: {error}')
            session.rollback()
        finally:
            session.close()

def main(args) -> None:
    if args.extract:
        simple_extract()

    if args.read_file:
        simple_read_file()

    if args.create:
        create_asteroids_table()
                
    if args.pipeline:
        make_pipeline()
                
if __name__ == '__main__':
    args = parse_arguments()
    main(args)