'''
The main script to combine the tables from the diverse sites into shared tables

Combines previous merge_users.py and concat_authmap.py plus new scripts

'''

import mysql.connector
import time
from common_func import *


def merge_users(outdbnm=SHARED_DB):
    """
    Merge users from all 6 Drupal sites into a single users table on the given database
    :param outdbnm: str (defualt is the SHARED_DB constant)
        the name of the database where the new concatenated user table will be created
    :return: None
    """
    # Build unified table data (identical with the previous merge_users.py script)
    users = {}
    keycol = 'name'

    # Go through all the sites and create a dictionary based on the key column
    # the dictionary is keyed on site name and each value is the row from that sites user table for the key column
    print("Compiling user list ....")
    for site in SITES:
        db = "{}{}".format(site, ENV)
        cnx = mysql.connector.connect(user='root', port=33067, database=db)
        cursor = cnx.cursor()
        cursor.execute('SELECT * FROM users')
        site_users = list(cursor)
        print("{} : {}".format(site, len(site_users)))
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
        newusr['signature_format'] = 'filtered_html' if newusr['signature_format'] is None else newusr[
            'signature_format']
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
        cursor.execute(sql, rvals)
        cnx.commit()
        c += 1

    # Adding back in the anonymous user (See http://drupal.org/node/1029506)
    cursor.execute(
        "INSERT INTO users (name, pass, mail, theme, signature, language, init, timezone) VALUES ('', '', '', "
        "'', '', '', '', '')")
    cursor.execute("UPDATE users SET uid = 0 WHERE name = ''")
    cnx.commit()

    print("{} users added to {} database".format(c, outdbnm))

    print("Creating the `uidcorresp` table with correspondences to old uids and new ones")

    cursor.execute('DROP TABLE IF EXISTS uidcorresp')
    corresp_tbl_schema = "CREATE TABLE `uidcorresp` ( " \
                         "`uid` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'Primary Key: the New Unique user ID.', \n" \
                         "`name` varchar(60) NOT NULL DEFAULT '' COMMENT 'Unique user name.', \n" \
                         "`mail` varchar(254) DEFAULT '' COMMENT 'User’s e-mail address.', \n"
    for site in SITES:
        corresp_tbl_schema += "`{}_uid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT " \
                              "'Old {} UID',".format(site, (site.replace("_", " ")).capitalize())

    # "`audio_video_uid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'Old AV UID'," \
    # "`images_uid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'Old Images UID', \n" \
    # "`mandala_uid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'Old Mandala UID', \n" \
    # "`sources_uid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'Old Sorces UID', \n" \
    # "`texts_uid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'Old Texts UID', \n" \
    # "`visuals_uid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT 'Old Visuals UID', \n" \

    corresp_tbl_schema += "`created` int(11) NOT NULL DEFAULT '0' COMMENT 'Timestamp for when user was created', \n" \
                          "PRIMARY KEY (`uid`), \n" \
                          "UNIQUE KEY `name` (`name`), \n" \
                          "KEY `mail` (`mail`) \n" \
                          ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Stores user id correspondences from old IDS to new one'"
    cursor.execute(corresp_tbl_schema)

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
        colstr = "(uid, name, mail, "
        for site in SITES:
            colstr += "{}_uid, ".format(site)
        colstr += 'created)'
        sql = 'INSERT INTO uidcorresp {} VALUES ({})'.format(colstr, ', '.join(['%s' for _ in range(len(uidupdata))]))
        res = cursor.execute(sql, uidupdata)
        cnx.commit()


def isauth(global_uid):
    """
    Determines if the user is listed in the authmap of any of the 6 sites
    :param global_uid: int
        the global user id is used to look up the correspondences in the correspondence table to find the users
        uid on each of the site with which to query the authmap table on each site respectively
    :return: bool
        True if user is in any of the authmap tables
    """
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


def concat_authmap():
    """
    Concats authmap authorization tables into a single table in the default database
    :return: None
    """
    users = getallrows(SHARED_DB, 'users')
    print("Truncated Shared Authmap table!")
    truncatetable(SHARED_DB, 'authmap')
    for usr in users:
        if isauth(usr['uid']):
            doinsert(SHARED_DB, 'authmap', 'uid, authname, module', (usr['uid'], usr['name'], DEFAULT_AUTH))
    rows = getallrows(SHARED_DB, 'authmap')
    print("Repopulated Shared Authmap table with {} rows".format(len(rows)))


def merge_roles():
    """
    Creates the new global roles on the default database
    Uses the DEFAULT_ROLES constant from common_func.py which defines these roles
    The constant ROL_COLL_NAMES are the column name for the roles table and
    The constant ROLE_CORRS define the correspondences between individual sites roles and the new set of global roles
    :return: None
    """
    truncatetable(SHARED_DB, 'role')
    for rdata in DEFAULT_ROLES:
        doinsert(SHARED_DB, 'role', str(ROL_COLL_NAMES), rdata)


def concat_users_roles():
    """
    Create a unified users_roles table in the default database for each global user by find that users entries in the
    various sites' users_roles table and using the highest level, translating it into the new role id
    Using the ROLE_CORRS constant in common_func.py
    :return: None
    """
    users = getallrows(SHARED_DB, 'users')


def merge_all_tables(db=SHARED_DB):
    """
    This function will perform all the required tasks to merge the necessary tables to create shared users and enable
    Single Sign-on. Ultimately, it will be this function that will be run on Stage and Production

    :param db: str
        name of the global database (default to the shared db)
    :return: None
    """
    merge_users(db)
    concat_authmap()
    merge_roles()


if __name__ == '__main__':
    print("Testing out a function...")

