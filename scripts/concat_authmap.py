'''
A script to concat the authmap tables.
Relies on the `uidcorresp` table in the shared db created by mergeusers.py

'''

import mysql.connector
from common_func import *


def isauth(global_uid):
    uids = getcorresps(global_uid)
    if uids is None:
        return False
    for site in SITES:
        db = "{}{}".format(site, ENV)
        mycnx = mysql.connector.connect(user='root', port=33067, database=db)
        mycur = mycnx.cursor()
        uidnm = "{}_uid".format(site)
        mycur.execute("SELECT * FROM authmap WHERE uid={}".format(uids[uidnm]))
        res = mycur.fetchone()
        if res is not None:
            if res[-1] == DEFAULT_AUTH or res[-1] == 'shib_auth':
                return True
    return False


# Main Program

users = getallrows(SHARED_DB, 'users')

truncatetable(SHARED_DB, 'authmap')

print("Truncated Shared Authmap table!")

ct = 0
for usr in users:
    if isauth(usr['uid']):
        doinsert(SHARED_DB, 'authmap', 'uid, authname, module', (usr['uid'], usr['name'], DEFAULT_AUTH))

rows = getallrows(SHARED_DB, 'authmap')
print("Repopulated Shared Authmap table with {} rows".format(len(rows)))

