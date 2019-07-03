'''
Common functions
'''
import mysql.connector

SITES = ('audio_video', 'images', 'mandala', 'sources', 'texts', 'visuals')
ENV = '_predev'
SHARED_DB = 'shanti_predev'
DEFAULT_AUTH = 'simplesamlphp_auth'


def doquery(db, query, return_type='dict'):
    mycnx = mysql.connector.connect(user='root', port=33067, database=db)
    mycrs = mycnx.cursor()
    mycrs.execute(query)
    if return_type == 'dict':
        return resdict(mycrs)
    else:
        return mycrs.fetchall()


def doinsert(db, tbl, cols, vals):
    if isinstance(cols, list):
        cols = ', '.join(cols)
    mycnx = mysql.connector.connect(user='root', port=33067, database=db)
    mycrs = mycnx.cursor()
    sql = 'INSERT INTO {} ({}) VALUES ({})'.format(
        tbl,
        cols,
        ', '.join(['%s' for _ in vals])
    )
    mycrs.execute(sql, vals)
    mycnx.commit()


def droptable(db, tbl):
    mycnx = mysql.connector.connect(user='root', port=33067, database=db)
    mycrs = mycnx.cursor()
    mycrs.execute('DROP TABLE IF EXISTS {}'.format(tbl))


def getcorresps(global_uid):
    mycnx = mysql.connector.connect(user='root', port=33067, database=SHARED_DB)
    mycrs = mycnx.cursor()
    mycrs.execute("SELECT * FROM uidcorresp WHERE uid={}".format(global_uid))
    res = resdict(mycrs)
    if len(res) > 0:
        return res[0]
    else:
        return None


def getallrows(db, tbl):
    mycnx = mysql.connector.connect(user='root', port=33067, database=db)
    mycrs = mycnx.cursor()
    mycrs.execute("SELECT * FROM {}".format(tbl))
    return resdict(mycrs)


def resdict(crs):
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
    mycnx = mysql.connector.connect(user='root', port=33067, database=db)
    mycrs = mycnx.cursor()
    mycrs.execute('TRUNCATE TABLE {}'.format(tbl))

