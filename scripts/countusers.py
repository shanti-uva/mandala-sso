import mysql.connector
import pandas as pd

SITES = ('audio_video', 'images', 'mandala', 'texts', 'sources', 'visuals')
TABLES = ('authmap', 'users', 'role', 'role_permission', 'users_roles', 'sessions')
SCHEMAS = {}

dbuser = 'drupaluser'
dbport = 33067
env = '_predev'
uvaid = 'ndg8f'
outdata = {'site': [],
            'users': [],
            'uva_users': []}

for site in SITES:
    outdata['site'].append(site)
    cnx = mysql.connector.connect(user=dbuser, port=dbport, database="{}{}".format(site, env))
    cursor = cnx.cursor()
    # Find number of users
    sql = 'SELECT count(*) FROM users'
    cursor.execute(sql)
    res = list(cursor.fetchone())
    outdata['users'].append(res[0])
    # Find number of UVA users
    sql = 'SELECT count(*) FROM users WHERE mail like "%virginia.edu%"'
    cursor.execute(sql)
    res = list(cursor.fetchone())
    outdata['uva_users'].append(res[0])

df = pd.DataFrame(outdata)
print(df)
