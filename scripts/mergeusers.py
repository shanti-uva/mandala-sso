'''
Script to merge user tables from sites db

'''
import mysql.connector

SITES = ('audio_video', 'images', 'mandala', 'texts', 'sources', 'visuals')
ENV = '_predev'

def createdb(dbnm):
    cnx = mysql.connector.connect(user='root', port=33067)
    cursor = cnx.cursor()

    cursor.execute('CREATE DATABASE {}'.format(dbnm))
    cnx.commit()
    print("done")



# Build unified table data
users = {}
keycol = 'mail'
names = {}
for site in SITES:
    db = "{}{}".format(site, ENV)
    cnx = mysql.connector.connect(user='root', port=33067, database=db)
    cursor = cnx.cursor()
    cursor.execute('SELECT * FROM users')
    site_users = list(cursor)
    print ("{} : {}".format(site, len(site_users)))
    for user in site_users:
        u = {}
        for idx, col in enumerate(cursor.column_names):
            u[col] = user[idx]
        if u['name'] in names.keys():
            names[u['name']][site] = {
                'uid': u['uid'],
                'mail': u['mail']
            }
        else:
            names[u['name']] = {
                site: {
                    'uid': u['uid'],
                    'mail': u['mail']
                }
            }
        if not u[keycol] in users:
            users[u[keycol]] = {}
        users[u[keycol]][site] = u

print("{}".format(len(users)))

for nm, data in names.items():
    mail = None
    for site in data.keys():
        if data[site]['mail'] is "":
            continue
        if mail is None:
            mail = data[site]['mail'].lower()
        elif data[site]['mail'].lower() != mail:
            print("{} : {}".format(nm, names[nm]))
            break

