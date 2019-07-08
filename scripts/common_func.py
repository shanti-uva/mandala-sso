'''
Common functions
'''
import mysql.connector

# Constants
SITES = ('audio_video', 'images', 'mandala', 'sources', 'texts', 'visuals')
ENV = '_predev'
SHARED_DB = 'shanti_predev'
DEFAULT_AUTH = 'simplesamlphp_auth'
DEFAULT_ROLES = [
    ('anonymous user', 1, 0),
    ('authenticated user', 2, 1),
    ('administrator', 3, 2),
    ('editor', 4, 3),
    ('workflow editor', 5, 4),
    ('shanti editor', 6, 5)
]
ROL_COLL_NAMES = ('name', 'rid', 'weight')
ROLE_CORRS = {
    'audio_video': {4: 4, 5: 6, 11: 5},
    'sources': {4: 4},
    'visual': {4: 6}
}


# Common Functions
def doquery(db, query, return_type='dict'):
    """
    Function to perform a query on a database
    :param db: str
        the database name
    :param query: str
        the full query string (no semicolon)
    :param return_type: str
        the type of return for each row, defaults to 'dict' for dictionary of column name, value pairs.
        Anything else returns the rows as the default mysql return type, tuples
    :return: list
        A list of result rows either as dictionaries or tuples
    """
    mycnx = mysql.connector.connect(user='root', port=33067, database=db)
    mycrs = mycnx.cursor()
    mycrs.execute(query)
    if return_type == 'dict':
        return resdict(mycrs)
    else:
        return mycrs.fetchall()


def doinsert(db, tbl, cols, vals):
    """
    Insert a single row into a database
    :param db: str
        the database name
    :param tbl: str
        the name of the table
    :param cols: list
        the columns for which values are given
    :param vals: list
        the values for each column
    :return: None
    """
    if isinstance(cols, list):
        cols = ', '.join(cols)
    if cols[0] != '(':
        cols = '(' + cols + ')'
    cols = cols.replace("'", '')
    mycnx = mysql.connector.connect(user='root', port=33067, database=db)
    mycrs = mycnx.cursor()
    sql = 'INSERT INTO {} {} VALUES ({})'.format(
        tbl,
        cols,
        ', '.join(['%s' for _ in vals])
    )
    mycrs.execute(sql, vals)
    mycnx.commit()


def droptable(db, tbl):
    """
    Drop a table from a database
    :param db: str
        the database name
    :param tbl: str
        the name of the table
    :return: None
    """
    mycnx = mysql.connector.connect(user='root', port=33067, database=db)
    mycrs = mycnx.cursor()
    mycrs.execute('DROP TABLE IF EXISTS {}'.format(tbl))


def find_uid(site, suid, rettype='int'):
    """
    Find a global uid for a person based on their old individual site uid
    :param site: str
        the name of the site as in the database name, e.g. "audio_video", "images"
    :param suid: int/str
        the users uid on the above site
    :param rettype: str
        the type of return desired, either "int" for the uid integer or "row" or "dict" for the whole row
    :return: dict
        the global user row as a
    """
    mycnx = mysql.connector.connect(user='root', port=33067, database=SHARED_DB)
    mycrs = mycnx.cursor()
    mycrs.execute('SELECT * FROM users WHERE {}_uid=\'{}\''.format(site, suid))
    rw = getfirstrow(mycrs)
    if rettype == "int":
        return rw['uid']
    else:
        return rw


def getcorresps(global_uid):
    """
    Get all the correspondences between a given global uid and all the various sites
    :param global_uid: int/str
        the global uid
    :return: dict
        a dictionary of column/value pairs from the correspondence table
    """
    mycnx = mysql.connector.connect(user='root', port=33067, database=SHARED_DB)
    mycrs = mycnx.cursor()
    mycrs.execute("SELECT * FROM uidcorresp WHERE uid={}".format(global_uid))
    return getfirstrow(mycrs)


def getallrows(db, tbl):
    """
    Gets all rows from a database table
    :param db: str
        the name of the database
    :param tbl:
        the name of the table
    :return: list of dictionaries
        a list of dictionaries for each row
    """
    mycnx = mysql.connector.connect(user='root', port=33067, database=db)
    mycrs = mycnx.cursor()
    mycrs.execute("SELECT * FROM {}".format(tbl))
    return resdict(mycrs)


def getfirstrow(crs):
    """
    Get the first row of results
    :param crs: mysql.connector.cursor_cext.CMySQLCursor
        a python mysql cursor object
    :return: tuple
        the first row of results from the cursor object
    """
    res = resdict(crs)
    if len(res) > 0:
        return res[0]
    else:
        return None


def resdict(crs):
    """
    Covert a python mysql query cursors' results into a dictionary
    :param crs: mysql.connector.cursor_cext.CMySQLCursor
        the python myswl cursor object after a query has been executed
    :return:
    """
    newres = []
    cols = crs.column_names
    rows = crs.fetchall()
    if len(rows) > 0:
        if len(cols) != len(rows[0]):
            raise KeyError("The number of columns ({}) do not match the number of items in the " /
                           "first row ({})".format(len(cols), len(rows[0])))
        for rw in rows:
            rwdict = {}
            for n, cnm in enumerate(cols):
                rwdict[cnm] = rw[n]
            newres.append(rwdict)

    return newres


def truncatetable(db, tbl):
    """
    Truncate a given table
    :param db: str
        The database name
    :param tbl: str
        The table name
    :return: None
    """
    mycnx = mysql.connector.connect(user='root', port=33067, database=db)
    mycrs = mycnx.cursor()
    mycrs.execute('TRUNCATE TABLE {}'.format(tbl))

