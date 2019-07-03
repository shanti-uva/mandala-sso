'''
Script to merge users into a single table
This script reads the dbs of all 6 sites, gathers there user information, concatenates user records based on mail
Then creates a new database (outdbnm is its name) and adds a user table to it.
It then writes a table of correspondences: uidcorresp with the new uid column as the primary key and name as unique key (as in Drupal)
with columns for that user's id in all the other sites: avuid, imguid, manuid, srcuid, txtuid, and visuid. The created column of that
table is when the record in that table was created
'''

import mysql.connector
import time

SITES = ('audio_video', 'images', 'mandala', 'sources', 'texts', 'visuals')
ENV = '_predev'
outdbnm = 'shanti_predev'

# Build unified table data
users = {}
keycol = 'name'
names = {}

# Go through all the sites and create a dictionary based on the key column
# the dictionary is keyed on site name and each value is the row from that sites user table for the key column
print("Compiling user list ....")
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
        if not u[keycol] in users:
            users[u[keycol]] = {}
        users[u[keycol]][site] = u

spec_cases = {
    'ShantiAdmin': 'audio_video',
    'wnm': 'audio_video',
    'et3p': 'audio_video',
    'am3zb': 'mandala',
    'Chelsea': 'visuals',
    'maw4fp': 'texts',
    'ndg8f': 'images'
}

# Filter/Fix User table by creating a new users dictionary
new_users = {}
uid = 0
for name, urecs in users.items():
    default_row = 'audio_video' if name not in spec_cases else spec_cases[name]
    if name == '':
        continue
    newusr = urecs[default_row] if default_row in urecs.keys() else urecs[list(urecs.keys())[0]]
    if newusr['status'] == 0:
        continue
    newusr['timezone'] = 'America/New_York' if newusr['timezone'] == 'Africa/Abidjan' \
        or newusr['timezone'] is None \
        or newusr['timezone'] == '' \
        else newusr['timezone']
    newusr['signature_format'] = 'filtered_html' if newusr['signature_format'] is None else newusr['signature_format']
    uid += 1
    newusr['uid'] = uid
    for db, row in urecs.items():
        if db == default_row:
            continue
        newusr['mail'] = row['mail'] if newusr['mail'] == '' else newusr['mail']
        newusr['created'] = row['created'] if row['created'] < newusr['created'] else newusr['created']
        newusr['access'] = row['access'] if row['access'] < newusr['access'] else newusr['access']
        newusr['login'] = row['login'] if row['login'] < newusr['login'] else newusr['login']

    if newusr['mail'] == '':
        newusr['mail'] = "{}@virginia.edu".format(newusr['name'])
    new_users[name] = newusr
    users[name]['newuid'] = uid  # for writing correspondences later

# Add rows to the new database
print("Writing users to {}’s user table".format(outdbnm))
cnx = mysql.connector.connect(user='root', port=33067, database=outdbnm)
cursor = cnx.cursor()
cursor.execute('TRUNCATE TABLE users')
c = 0
for name, rwdata in new_users.items():
    if name is '' or 'thread_' in name:
        continue

    rkeys = []
    rvals = []
    for k, v in rwdata.items():
        if k == 'uuid':
            # print ("uuid: {}, {}".format(name, rwdata))
            continue
        rkeys.append(k)
        rvals.append(v)

    sql = 'INSERT INTO users ({}) VALUES ({})'.format(
        ', '.join(rkeys),
        ', '.join(['%s' for rv in rvals])
    )
    # if c == 0:
    #     print(sql)
    #     print("{}: {}".format(name, rvals))
    cursor.execute(sql, rvals)
    cnx.commit()
    c += 1

print("{} users added to {} database".format(c, outdbnm))

print("Creating the `uidcorresp` table with correspondences to old uids and new ones")

corresp_tbl_schema = "CREATE TABLE IF NOT EXISTS `uidcorresp` ( " \
  "`uid` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'Primary Key: the New Unique user ID.', \n" \
  "`name` varchar(60) NOT NULL DEFAULT '' COMMENT 'Unique user name.', \n" \
  "`mail` varchar(254) DEFAULT '' COMMENT 'User’s e-mail address.', \n" \
  "`avuid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'Old AV UID'," \
  "`imguid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'Old Images UID', \n" \
  "`manuid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'Old Mandala UID', \n" \
  "`srcuid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'Old Sorces UID', \n" \
  "`txtuid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'Old Texts UID', \n" \
  "`visuid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'Old Visuals UID', \n" \
  "`created` int(11) NOT NULL DEFAULT '0' COMMENT 'Timestamp for when user was created', \n" \
  "PRIMARY KEY (`uid`), \n" \
  "UNIQUE KEY `name` (`name`), \n" \
  "KEY `mail` (`mail`) \n" \
  ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Stores user id correspondences from old IDS to new one'"
cursor.execute(corresp_tbl_schema)
cursor.execute('TRUNCATE TABLE uidcorresp')

ct = 0
for nm, usr in users.items():
    if nm is None or nm == '':
        continue
    if nm not in new_users:
        print("{} is not found in the new_users dictionary".format(nm))
        continue
    ct += 1
    newusr = new_users[nm]
    uidupdata = [newusr['uid'], newusr['name'], newusr['mail']]
    sites = list(SITES)
    sites.sort()
    for site in sites:
        sid = usr[site]['uid'] if site in usr else 0
        uidupdata.append(sid)
    uidupdata.append(int(time.time()))
    sql = 'INSERT INTO uidcorresp (uid, name, mail, avuid, imguid, manuid, srcuid, txtuid, visuid, created) ' \
           'VALUES ({})'.format(', '.join(['%s' for _ in range(len(uidupdata))]))
    res = cursor.execute(sql, uidupdata)
    cnx.commit()

