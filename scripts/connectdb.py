import mysql.connector

SITES = ('audio_video', 'images', 'mandala', 'texts', 'sources', 'visuals')

def dbconnect(site, env='_predev'):
    dbuser = 'drupaluser'
    dbport = 33067
    db = "{}{}".format(site, env)
    cnx = mysql.connector.connect(user=dbuser, port=dbport, database=db)
    cursor = cnx.cursor()
    return cursor
