'''
This script is to concatenate all the entries from the various role tables
'''

import mysql.connector

SITES = ('audio_video', 'images', 'mandala', 'sources', 'texts', 'visuals')
ENV = '_predev'
outdbnm = 'mergetestdb'

roles = {}

for site in SITES:
    roles[site] = {}
    db = "{}{}".format(site, ENV)
    cnx = mysql.connector.connect(user='root', port=33067, database=db)
    cursor = cnx.cursor()
    cursor.execute('SELECT * FROM role')
    sroles = cursor.fetchall()
    for row in sroles:
        rid = row[0]
        roles[site][rid] = row[1]

for k, v in roles.items():
    print("{}:".format(k))
    rids = list(v.keys())
    rids.sort()
    for rid in rids:
        print("\t{} : {}".format(rid, v[rid]))
