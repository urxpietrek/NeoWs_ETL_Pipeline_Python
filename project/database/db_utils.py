import pymysql as db
import sqlalchemy
import constants.queries as const

from project.utils import logger

def create_database(conn_config: dict):
    try:
        connection = db.connect(**conn_config)
        with connection.cursor() as cursor:
            cursor.execute(const.CREATE_NEOWS_DATABASE)
            connection.commit()
        logger.info('Database created succesfully.')
    except db.Error as e:
        logger.error(f'Failed to create database: {e}')
    finally:
        connection.close()

def database_exists(connection, db_name):
    """Function check if database exists."""
    
    query = (
        """\
        SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA\
        WHERE SCHEMA_NAME = %s
        """
    )
    with connection.cursor() as cursor:
        cursor.execute(query, db_name)
        return cursor.fetchone() is not None

def table_exists(engine: sqlalchemy.Engine, table_name:str ):
    """Function check if table exists."""
    
    query = (
        """\
        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES\
        WHERE TABLE_SCHEMA = :db_name AND TABLE_NAME = :table_name
        """
    )
    db_name = engine.url.database
    with engine.connect() as connection:
        result = connection.execute(
            sqlalchemy.text(query), {'db_name': db_name, 'table_name': table_name}
        )
        return result.fetchone() is not None