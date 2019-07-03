import mysql.connector
import phpserialize
import json

SITES = ('audio_video', 'images', 'mandala', 'texts', 'sources', 'visuals')
TABLES = ('authmap', 'users', 'role', 'role_permission', 'users_roles', 'sessions')
SCHEMAS = {}

dbuser = 'drupaluser'
dbport = 33067
env = '_predev'
uvaid = 'ndg8f'

for site in SITES:
    cnx = mysql.connector.connect(user=dbuser, port=dbport, database="{}{}".format(site, env))
    cursor = cnx.cursor()
    sql = 'SELECT * FROM users WHERE mail LIKE "%{}%"'.format(uvaid)
    cursor.execute(sql)
    res = list(cursor.fetchone())
    obj = phpserialize.loads(res[15].encode(), object_hook=phpserialize.phpobject)
    nobj = {}
    if obj:
        for k in obj.keys():
            nk = str(k.decode())
            nval = obj.get(k)
            if type(nval) == bytes:
                nval = nval.decode()
            nobj[nk] = str(nval)

    try:
        print("{} : {} : {}".format(site, res[1], res[3]))
        print(json.dumps(nobj))
    except AttributeError as ae:
        pass


