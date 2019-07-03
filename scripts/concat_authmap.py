'''
A script to concat the authmap tables.
Relies on the `uidcorresp` table in the shared db created by mergeusers.py

'''

import mysql.connector

SITES = ('audio_video', 'images', 'mandala', 'sources', 'texts', 'visuals')
ENV = '_predev'
SHARED_DB = 'shanti_predev'


def resdict(crs):
    newres = []
    cols = crs.column_names
    rows = crs.fetchall()
    if len(cols) != len(rows[0]):
        raise KeyError("The number of columns ({}) do not match the number of items in the " /
                       "first row ({})".format(len(cols), len(rows[0])))
    for rw in rows:
        rwdict = {}
        for n, cnm in enumerate(cols):
            rwdict[cnm] = rw[n]
        newres.append(rwdict)

    return newres


def getalluids(global_uid):
    print(SHARED_DB)
    mycnx = mysql.connector.connect(user='root', port=33067, database=SHARED_DB)
    mycrs = mycnx.cursor()
    mycrs.execute("select * from uidcorresp where uid={}".format(global_uid))
    res = resdict(mycrs)
    return res[0]


cnx = mysql.connector.connect(user='root', port=33067, database=SHARED_DB)
cursor = cnx.cursor()
cursor.execute('select * from users')
users = resdict(cursor)

for usr in users:
    if usr['name'] == 'ndg8f':
        mycorr = getalluids(usr['uid'])
        print(mycorr)

