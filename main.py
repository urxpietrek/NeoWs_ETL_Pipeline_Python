import sqlalchemy
import sqlalchemy.exc
from sqlalchemy.orm import sessionmaker
import pymysql as db

from config import settings

from project import NeoWsExtractor, Pipeline
from project.arguments import parse_arguments
from project.utils import logger
from project.utils import (
    get_mysql_engine,
    process_file
)

from project.database import (
    database_exists,
    table_exists,
    create_database
)

import constants.queries as const

API_KEY = settings['API_KEY']

def get_session(engine: sqlalchemy.Engine):
    """
        Function which returns session with binded engine
    """
    return sessionmaker(bind=engine)

SessionFactory = get_session(get_mysql_engine(**settings.database))

                    
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
    try:
        start_date, end_date = args.extract.split(' ') if len(args.extract.split(' ')) > 1 else (args.extract, None)
        extractor = NeoWsExtractor(API_KEY)
        extraction_result = extractor.extract(start_date, end_date)
        logger.info('Data extraction completed successfully.')
        print(extraction_result)
    except Exception as e:
        logger.error(f'Error during extraction: {e}')
    
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
            
def do_pipeline():
    pipeline = Pipeline(API_KEY)
    try:
        start_date, end_date = (args.pipeline.split(' ')
                            if len(args.pipeline.split(' ')) > 1
                            else (args.pipeline, None))
        
        pipeline.do_pipeline(SessionFactory, start_date, end_date)
        logger.info('Pipeline execution completed successfully.')
    except Exception as e:
        logger.error(f'Pipeline execution failed: {e}')
        
def main(args) -> None:
    if args.extract:
        simple_extract()

    if args.read_file:
        simple_read_file()

    if args.create:
        create_asteroids_table()
                
    if args.pipeline:
        do_pipeline()
                
if __name__ == '__main__':
    args = parse_arguments()
    main(args)