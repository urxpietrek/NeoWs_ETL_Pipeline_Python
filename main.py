import sqlalchemy
import sqlalchemy.exc
from sqlalchemy.orm import sessionmaker
from config import settings
from project.utils import (
    extract_data_from_json,
    get_available_files,
    get_mysql_engine
)
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
                continue  # Consider logging this exception in production code

    return aggregated_records
             
                    
def create_asteroids_table() -> bool:
    """
        Function creates asteroid table in a given database parameters.
    """
    engine: sqlalchemy.Engine = get_mysql_engine(**settings.database)
    try:
        with engine.connect() as connection:
            with connection.begin() as trans:
                connection.execute(sqlalchemy.text(
                                    const.CREATE_ASTEROIDS_OBSERVATIONS_TABLE)
                                    )
                
                trans.commit()
                print('Tabela utworzona')
                return True
                
    except sqlalchemy.exc.SQLAlchemyError as e:
        print(f"Database error: {e}")
        if 'trans' in locals():
            trans.rollback()
            return False


def main(args) -> None:
    if args.extract:
        start_date, end_date = args.extract if len(args.extract) else (args.extract, None)
        extractor = NeoWsExtractor(API_KEY)
        extraction_result = extractor.extract(start_date, end_date)
        print(extraction_result)

    if args.read_file:
        try:
            from project.model import TargetData
        except ImportError:
            return

        file_path = args.read_file
        records = process_file(file_path)

        if records:
            try:
                from sqlalchemy.dialects.mysql import insert
                with SessionFactory() as session:
                    session.bulk_insert_mappings(TargetData, records)
                    session.commit()
                    print('Data inserted successfully')
            except sqlalchemy.exc.SQLAlchemyError as error:
                print(f'Error during data insertion: {error}')
                session.rollback()
            finally:
                session.close()

    if args.create:
        create_asteroids_table()
                
                
if __name__ == '__main__':
    args = parse_arguments()
    main(args)