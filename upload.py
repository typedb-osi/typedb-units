from typedb.client import *
from loguru import logger
from csv import DictReader

# Blending TypeQL, and one layer of data
# import form for each data object
class TypeQL_Data_Slice:
    def __init__(self, data):
        self.data = data
        self.dimensions = ['dim-t', 'dim-l', 'dim-m', 'dim-i', 'dim-k', 'dim-n', 'dim-j', 'dim-r', 'dim-c']
        self.units = ['s', 'm', 'kg', 'A', 'k', 'mol', 'cd', 'rad', 'kg/kg' ]

    def core(self):
        statement = 'match '
        dims = []
        
        for i, dim in enumerate(self.dimensions):
            if ( self.data[dim]  != '0'):
                statement += '$'+ dim + ' isa ' + dim + ';$'+ dim + ' '+ self.data[dim]
                statement += '; $' + dim + ' has unit "' + self.units[i] + '";'
                dims.append(dim)

        if self.data["isCore"] == "FALSE":
            # match in the core role
            statement += '$core isa unit_details, has unit "'+ self.data["core"] + '";'

        statement += 'insert $unit_details ('
        for dim in dims:
            statement += ' dimensions: ' + '$' + dim + ', '

        size = len(statement)
        statement = statement[:size - 2]

        if self.data["isCore"] == "FALSE":
            # add in the core role
            statement += ', core: $core '

        statement += ') isa unit_details, has unit "' + self.data["unit"] + '",'
        statement += 'has uname "' + self.data["uname"] + '",'
        statement += 'has utype "' + self.data["utype"] + '",'
        statement += 'has ctype "' + self.data["ctype"] + '",'
        statement += 'has c_first ' + self.data["c_first"] + ','
        statement += 'has c_second ' + self.data["c_second"] + ','
        statement += 'has isCore ' + self.data["isCore"].lower() + ','
        statement += 'has latex_string "' + self.data["latex_string"] + '",'
        statement += 'has description "' + self.data["description"] + '";'   
        logger.debug(f'inserted the unit -> {self.data["unit"]}')                    
        return statement

    def derived(self):
        return self.core()

    def not_core(self):
        return self.core()

    def scaled(self):
        return self.core()
    
    def imperial(self):
        return self.core()

    def examples(self):
        statement = 'insert '
        statement += '$measure isa measure; $measure ' + self.data["measure"] + '; '
        statement += '$measure has unit "' + self.data["unit"] + '"; '
        return statement


    def dimension (self):
        statement = 'insert $dim isa ' + self.data["dim"] + '; '
        statement += '$dim ' + self.data["value"] + '; '
        statement += '$dim has unit "' + self.data["unit"] + '"; '
        return statement

        
    
    


# upload a csv_dict_reader object of data for a typeql object
def upload(tql_obj_name, csv_dict_reader, session):
    """ Upload a array of dicts through the session

    :param tql_obj_name: type of obejct to add
    :type tql_obj_name: string
    :param csv_dict_reader: data collection to be loaded
    :type csv_dict_reader: array of dicts
    :param session: valid typeDB write session
    :type session: typeDB session object

    :returns: nothing
    :rtype: void
    """
    # We adopt the strategy of committing the transaction on every slice of the data
    # total = len(csv_dict_reader)
    total = 0
    for count, data_slice in enumerate(csv_dict_reader):
        total += 1
        # integrate the typeQL + data slice through the object name
        # logger.debug(f'slice is -> {data_slice}')
        new_slice = TypeQL_Data_Slice(data_slice)
        query_slice = getattr(new_slice, tql_obj_name)()
        # write the statement into TypeDB        
        with session.transaction(TransactionType.WRITE) as write_transaction:
            insert_iterator = write_transaction.query().insert(query_slice)
            # logger.debug(f'-------------- Start Insert Slice of {tql_obj_name} , {count} of {total} ---------------------')
            # logger.debug(f'Query Slice in Statement - {query_slice}')
            """ for concept_map in insert_iterator:
                    concepts = concept_map.concepts()
                    #logger.debug("Inserted an event with name: {0}".format(concepts[0].id))
                    #logger.debug("Inserted an event with name: {0}".format(concepts[0].id))
                    ## to persist changes, write transaction must always be committed (closed)
                    #logger.debug("------ Insert Trace Statement")
                    #logger.debug(f"Graql --> {graql_insert}")
                    #logger.debug("------ Response")
                    for c in concepts:
                        if(c.is_attribute()):
                            logger.debug(f'Attribute value is -> {c.get_value()}')
                        elif(c.is_relation()):
                            rel_id = c.get_iid()
                            logger.debug(f"relation id is -> {rel_id}")
                        else:
                            logger.debug(f"entity {c.get_iid()}")
                    logger.debug("------ End Query Slice Statement") """
                    
            # logger.debug("------ End Insert  Slice Statement---------------------------------")
            # logger.debug(f' statement is -> {query_slice}')
            write_transaction.commit()


    
 
def import_file(iterator, total, path_to_csv, name_list, session):
    """ Opens a CSV File in a directory

    :param iterator: base on which to add iteration
    :type iterator: integer
    :param total: total of files to load
    :type total: integer
    :param path_to_csv: string of local path to data
    :type path_to_csv: string
    :param name_list: array of object names to load
    :type name_list: array of string
    :param session: valid typeDB write session
    :type session: typeDB session object

    :returns: nothing
    :rtype: void
    """
    for count, obj_type in enumerate(name_list):
                number = str(iterator + count + 1)
                filename = path_to_csv + obj_type + '.csv'
                with open(filename) as f:
                    csv_dict_reader = DictReader(f)
                    logger.debug(f'---------- Import {number} of {total} CSV files ------------- {filename} -------------------')
                    upload(obj_type, csv_dict_reader, session)


def import_files(server, path_to_csv, initial, primary,secondary):
    """ Opens CSV Files in a directory in an order

    :param server: dict of server connection details
    :type server: dict of strings
    :param path_to_csv: string of local path to data
    :type path_to_csv: string
    :param primary: array of first to load object names
    :type primary: array of string
    :param secondary: array of match then load object names
    :type secondary: string

    :returns: nothing
    :rtype: void
    """
    typedb_connection = server["url"] + ":" + server["port"]
    with TypeDB.core_client(typedb_connection) as client:
        with client.session(server["database"], SessionType.DATA) as session:
            base = 1
            second = len(primary) + 1
            total = second + len(secondary) + 1
            import_file(0, total, path_to_csv, initial, session)
            import_file(base, total, path_to_csv, primary, session)
            import_file(second, total, path_to_csv, secondary, session)
            

    

# if this file is run directly, then start here
if __name__ == '__main__':
    # define the database server and import details
    server = {
        "url": "localhost",
        "port": "1729",
        "database": "units"
    }
    path_to_csv = './data/'
    initial = ["dimension"]
    primary = ['base','core','not_core']
    secondary = ['scaled', 'imperial', 'examples']    
    
    # load all of the files in the director
    logger.debug("=================== Start DB Import ===========================")
    import_files(server, path_to_csv, initial, primary, secondary)
    logger.debug("=================== End DB Import ===========================")
    