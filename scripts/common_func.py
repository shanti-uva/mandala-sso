'''
Common functions
'''
import mysql.connector

# Constants
SITES = ('audio_video', 'images', 'mandala', 'sources', 'texts', 'visuals')  # all the resource sites in Drupal
# TODO: Update ENV and SHARE_DB to work in the Acquia Cloud environment. The current set up is for DevDesktop.
ENV = '_predev'                         # the current working environment
SHARED_DB = 'shanti_predev'             # the destination database where the shared tables will reside
DEFAULT_AUTH = 'simplesamlphp_auth'     # default type of authorization
DEFAULT_ROLES = [                       # A list of tuples defining the new roles in the shared tables
    ('anonymous user', 1, 0),
    ('authenticated user', 2, 1),
    ('administrator', 3, 2),
    ('editor', 4, 3),
    ('workflow editor', 5, 4),
    ('shanti editor', 6, 5)
]
ROL_COLL_NAMES = ('name', 'rid', 'weight')  # The columns for the roles table
ROLE_CORRS = {                              # The correspondences bet. individual sites' roles and the global roles
    'audio_video': {4: 4, 5: 6, 11: 5},
    'sources': {4: 4},
    'texts': {11: 4},
    'visuals': {4: 6, 5: 4}
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
        the type of return for each row, defaults to 'dict'
            "dict" : returns a list of rows as dictionary of keys-value pairs, where keys are the column names
            "val"  : returns a single value
            ""     : returns a list of rows as tuples (the mysql default)
        Anything else returns the rows as the default mysql return type, tuples
    :return: list
        A list of result rows either as dictionaries or tuples
    """
    mycnx = mysql.connector.connect(user='root', port=33067, database=db)
    mycrs = mycnx.cursor()
    mycrs.execute(query)
    if return_type == 'dict':
        return resdict(mycrs)
    elif return_type == 'list':
        res = [item[0] for item in mycrs.fetchall()]
        return res
    elif return_type == 'val':
        res = mycrs.fetchone()
        if res is None or len(res) == 0:
            return False
        else:
            return res[0]
    elif return_type == 'commit':
        mycnx.commit()
    else:
        return mycrs.fetchall()


def doinsert(db, tbl, cols, vals):
    """
    Insert a single row into a database
    :param db: str
        the database name
    :param tbl: str
        the name of the table
    :param cols: str or list
        the columns for which values are given
    :param vals: list
        the values for each column
    :return: None
    """
    if isinstance(cols, list) or isinstance(cols, tuple):
        cols = list(map(lambda x: str(x), list(cols)))  # convert whatever is given into an list of strings
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


def doinsertmany(db, tbl, cols, vals):
    """

    :param db: str
        database name
    :param tbl: str
        table nabme
    :param cols: str or list
        columns
    :param vals: list
        list of rows (tuples) to insert
    :return: None
    """
    # Turn cols array into string surrounded by parentheses
    if isinstance(cols, list):
        cols = ', '.join(cols)
    if cols[0] != '(':
        cols = '(' + cols + ')'
    cols = cols.replace("'", '')

    # Create format string for values made of parentheses with %s for each item in row
    fmtvals = "({})".format(', '.join(['%s' for _ in vals[0]]))

    # Build query
    qry = 'INSERT INTO {} {} VALUES {}'.format(
        tbl,
        cols,
        fmtvals,
    )
    # print("query is: {}".format(qry))

    # Connect to database and execute a multi row insert
    mycnx = mysql.connector.connect(user='root', port=33067, database=db)
    mycrs = mycnx.cursor()
    mycrs.executemany(qry, vals)
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


def getcorrespsbysite(site, suid):
    """
    Get correspondences for user on a specific site
    :param site:
    :param suid:
    :return:
    """
    qry = "SELECT * from uidcorresp where {}_uid={}".format(site, suid)
    res = doquery(SHARED_DB, qry)
    return res


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
    if tableexists(db, tbl):
        mycnx = mysql.connector.connect(user='root', port=33067, database=db)
        mycrs = mycnx.cursor()
        mycrs.execute("SELECT * FROM {}".format(tbl))
        return resdict(mycrs)
    else:
        return {}


def getfirstrow(crs):
    """
    Get the first row of results
    :param crs: mysql.connector.cursor_cext.CMySQLCursor
        a python mysql cursor object
    :return: tuple
        the first row of results from the cursor object
    """
    res = resdict(crs, 1)
    if len(res) > 0:
        return res[0]
    else:
        return None


def getalluids(db=SHARED_DB):
    if ENV not in db:
        db = "{}{}".format(db, ENV)
    qry = "SELECT uid from users"
    res = doquery(db, qry, 'list')
    return res


def loaduser(uid, site=SHARED_DB):
    """
    Load a user from the selected site
    :param uid: int or str
        The user id to load or user name
    :param site: str
        The site db to search. Defaults to the shared db / global users table
    :return: dict
        The row representing that user's record or None if not found
    """
    qry = ''
    try:
        db = site if site == SHARED_DB else "{}{}".format(site, ENV)
        mycnx = mysql.connector.connect(user='root', port=33067, database=db)
        mycrs = mycnx.cursor()
        cond = 'uid={}'.format(uid) if isinstance(uid, int) or uid.isnumeric() else 'name="{}"'.format(uid)
        qry = "SELECT * FROM users WHERE {}".format(cond)
        mycrs.execute(qry)
        fr = getfirstrow(mycrs)
        return fr
    except mysql.connector.errors.ProgrammingError as pe:
        print("errorr: {}".format(qry))


def loadalluserinfo(uid):
    uinfo = loaduser(uid)
    corrs = getcorresps(uid)
    for site in SITES:
        uinfo["{}_user".format(site)] = loaduser(corrs["{}_uid".format(site)], site)
    return uinfo


def pout(str, lvl=1):
    print("{}{}".format("\t" * (lvl - 1), str))


def resdict(crs, limit_rows=-1):
    """
    Covert a python mysql query cursors' results into a dictionary
    :param crs: mysql.connector.cursor_cext.CMySQLCursor
        the python myswl cursor object after a query has been executed
    :return:
    """
    newres = []
    cols = crs.column_names
    if limit_rows == 1:
        row = crs.fetchone()
        rows = [row]
    else:
        rows = crs.fetchall()
    # print(rows)
    if rows and len(rows) > 0:
        if len(cols) != len(rows[0]):
            raise KeyError("The number of columns ({}) do not match the number of items in the " /
                           "first row ({})".format(len(cols), len(rows[0])))
        for rw in rows:
            rwdict = {}
            for n, cnm in enumerate(cols):
                rwdict[cnm] = rw[n]
            newres.append(rwdict)

    return newres


def tableexists(db, tbl):
    qry = "SHOW TABLES LIKE '{}'".format(tbl);
    res = doquery(db, qry, 'val')
    if res == tbl:
        return True
    else:
        return False


def translaterole(site, rid):
    """
    Translate a specific site role id into global role id using the ROLE_CORRS constant
    :param site: str
        machine name of the site
    :param rid: int
        the role id
    :return: int
        the corresponding global role id or -1 if not found
    """
    try:
        if site in ROLE_CORRS:
            return ROLE_CORRS[site][rid]
        else:
            return -1

    except KeyError as ke:
        print("{} | {}".format(site, rid))


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


def unbyte(obj):
    """
    Function to turn a byte based object into a string based object since unserialized blobs return the former
    For use with dictionaries primarily

    :param obj:
    :return:
    """
    if isinstance(obj, bytes):
        return obj.decode('utf-8')

    elif isinstance(obj, list):
        newlist = []
        for bitem in obj:
            newlist.append(unbyte(bitem))
        return newlist

    elif isinstance(obj, dict):
        newdict = {}
        for k,v in obj.items():
            nk = unbyte(k)
            nv = unbyte(v)
            newdict[nk] = nv
        return newdict

    elif isinstance(obj, int) or obj is None or isinstance(obj, bool):
        return obj

    else:
        print("Unknown type of object. {} is a {}".format(obj, type(obj)))
        return obj
