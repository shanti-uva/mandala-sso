'''
This script compares a single user across all the sites
'''
import mysql.connector
import pandas as pd
from datetime import datetime

SITES = ('audio_video', 'images', 'mandala', 'texts', 'sources', 'visuals')
env = '_predev'
usrnm = 'ndg8f'
fields = ['uid', 'name', 'mail', 'theme', 'timezone', 'language', 'status', 'picture']
outdata = []

for site in SITES:
    dbuser = 'drupaluser'
    dbport = 33067
    db = "{}{}".format(site, env)
    cnx = mysql.connector.connect(user=dbuser, port=dbport, database=db)
    cursor = cnx.cursor()
    sql = 'SELECT {} FROM users WHERE mail LIKE "%{}%"'.format(', '.join(fields), usrnm)
    cursor.execute(sql)
    res = list(cursor.fetchone())
    outdata.append(res)
    # for n in range(5, 8):
    #   res[n] = datetime.fromtimestamp(res[n])

pd.set_option('max_colwidth', 500)
pd.set_option('display.width', None)
df = pd.DataFrame(outdata, SITES, fields)
print(df)
