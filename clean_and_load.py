from typedb.client import *
from loguru import logger
import json
import upload as up


@logger.catch
def clean_and_load(server, path, initial, primary, secondary):
    logger.debug("======================== Check if DB Exists and Clean ======================")
    host = server["url"] + ":" + server["port"]
    db_name = server["database"]
    with TypeDB.core_client(host) as client:
        # does the cydalics db exist?
        logger.debug("---- client has started ------")
        db_exists = client.databases().contains(db_name)
        logger.debug(f'db exists is {db_exists}')
        # if db exists, then delete, else do nothing
        if (db_exists):
            logger.debug(f'the {db_name} database is being deleted')
            client.databases().get(db_name).delete()

        # in either case, create the new cydalics database
        client.databases().create(db_name)
        # load the current schema file into a string
        with open(server["schema"], 'r') as f:
            schema_string = ""
            for line in f:
                if line.startswith('#'):
                    continue

                line = line.partition('#')[0]
                line = line + '\n'
                schema_string += line

        # now start  the schema writing transaction
        with client.session(db_name , SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as write_transaction:
                define_iterator = write_transaction.query().define(schema_string)
                # logger.debug(f'Schema Define Statement - {schema_string}')
                logger.debug(f'::::::::::::::::::::::::::::::::')
                # logger.debug(f'define iterator -> {define_iterator}')
                
                        
                write_transaction.commit()


    logger.debug("=================== Start Units  Import ===========================")
    up.import_files(server, path, initial, primary, secondary)
    logger.debug("=================== End Units Import ===========================")


# if this file is run directly, then start here
if __name__ == '__main__':
    # define the database server and import details
    server = {
        "url": "localhost",
        "port": "1729",
        "database": "units",
        "schema": "./schema/units.tql"
    }
    path_to_file = './data/'
    initial = ["dimension"]
    primary = ['core','derived','not_base']
    secondary = ['scaled', 'imperial', 'examples']    
    
    # clean db and load all of the files in the director
    clean_and_load(server, path_to_file, initial, primary, secondary)
    