'''
Script to create the user table

'''
import mysql.connector

def createdb(dbnm):
    cnx = mysql.connector.connect(user='root', port=33067)
    cursor = cnx.cursor()

    cursor.execute('CREATE DATABASE {}'.format(dbnm))
    cnx.commit()

indbnm = 'audio_video_predev'
cnx = mysql.connector.connect(user='root', port=33067, database=indbnm)
cursor = cnx.cursor()
cursor.execute('SHOW CREATE TABLE users')
schema = cursor.fetchone()[1]

outdbnm = 'mergetestdb'
cnx = mysql.connector.connect(user='root', port=33067, database=outdbnm)
cursor = cnx.cursor()
cursor.execute(schema)

