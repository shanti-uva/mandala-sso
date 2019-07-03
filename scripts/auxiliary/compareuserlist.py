'''
Script to compare user lists across sites

'''
import mysql.connector

SITES = ('audio_video', 'images', 'mandala', 'texts', 'sources', 'visuals')
ENV = '_predev'



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


for k, v in names.items():
    if len(v.keys()) > 1:
        ml = None
        for s, d in v.items():
            if d['mail'] == '':
                continue
            sml = d['mail'].lower()
            if ml is None:
                ml = sml
            elif ml != sml:
                print("{}::".format(k))
                for n, m in v.items():
                    print("\t{} ({}): {}".format(n, m['uid'], m['mail']))
                break
