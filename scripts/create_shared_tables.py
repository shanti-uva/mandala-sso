'''
The main script to combine the tables from the diverse sites into shared tables

Combines previous merge_users.py and concat_authmap.py plus new scripts

'''

#  import mysql.connector
import time
from common_func import *
import pandas as pd
from pprint import PrettyPrinter
import phpserialize as php
import json

pd.set_option('display.max_rows', 200)
pp = PrettyPrinter(indent=2)


def compile_user_list(keycol):
    users = {}
    for site in SITES:
        db = "{}{}".format(site, ENV)
        site_users = getallrows(db, 'users')
        pout("{} : {}".format(site, len(site_users)), 2)
        for usr in site_users:
            if not usr[keycol] in users:
                users[usr[keycol]] = {}
            users[usr[keycol]][site] = usr
    return users


def merge_users(outdbnm=SHARED_DB):
    """
    Merge users from all 6 Drupal sites into a single users table on the given database
    :param outdbnm: str (defualt is the SHARED_DB constant)
        the name of the database where the new concatenated user table will be created
    :return: None
    """

    keycol = 'name'  # the column of a users record to key the compiled list on: the user name

    # Build unified table data first

    # Go through all the sites and create a dictionary of dictionaries
    # The main key is the key column above, e.g. "name" or "uid". The secondary key is the site name.
    # With that site's user record under its name key. For instance:
    #    users['ndg8f'] = {
    #          'audio_video': { av usr object },
    #          'images': {image usr object},
    #           ... etc ...
    #    }

    pout("Compiling user list ....", 2)
    compiled_users = compile_user_list(keycol)

    # Special cases: for particular people (user names) use a specific site for the user info
    spec_cases = {
        'am3zb': 'mandala',
        'Chelsea': 'visuals',
        'maw4fp': 'texts',
        'ndg8f': 'images'
    }

    # Create new_users dictionary from the compiled user list above keyed on the same value
    new_users = {}          # the dictionary for new user records (keyed on user name)
    corresps = {}           # the dictionary for correspondences between global and site uids (keyed on new global uid)
    uid = 0                 # the counter that determines the global UID. (trusting that ShantiAdmin is always 1)

    # Iterate through compiled user dictionary to create new_users dictionary
    for ukey, urecs in compiled_users.items():
        # ukey is the key used in the users dictionary in this case the name,
        # urecs is a dictionary of user records keyed by site
        # create the newuser object based on the "default" row/site in the urecs
        # then add it to the new_users dictionary with the same key

        # Skip anonymous or the old "thread_000" users
        if ukey is '' or 'thread_' in ukey:
            continue

        # Choose the default site to use for base user record for this persons
        default_row = 'audio_video' if ukey not in spec_cases else spec_cases[ukey]
        if ukey == '':
            continue

        # Set newusr to the record of the default site, av, or else the first key listed
        # Notice: with Python dictionaries one needs to explicitly call the copy routine
        #         to avoid newusr being just a reference to the dictionary in compiled_users
        newusr = urecs[default_row].copy() if default_row in urecs.keys() else urecs[list(urecs.keys())[0]].copy()
        if newusr['status'] == 0:
            continue

        # Fix erroneous timezones
        newusr['timezone'] = 'America/New_York' if newusr['timezone'] == 'Africa/Abidjan' \
            or newusr['timezone'] is None or newusr['timezone'] == '' \
            else newusr['timezone']
        newusr['signature_format'] = 'filtered_html' if newusr['signature_format'] is None \
            else newusr['signature_format']
        uid += 1
        newusr['uid'] = uid

        # Use the earliest created timestamp and latest access and login from all sites user belongs to
        for db, row in urecs.items():
            if db == default_row:
                continue
            newusr['mail'] = row['mail'] if newusr['mail'] == '' else newusr['mail']
            newusr['created'] = row['created'] if row['created'] < newusr['created'] else newusr['created']
            newusr['access'] = row['access'] if row['access'] < newusr['access'] else newusr['access']
            newusr['login'] = row['login'] if row['login'] < newusr['login'] else newusr['login']

        if newusr['mail'] == '' and newusr['name'] != '':
            newusr['mail'] = "{}@virginia.edu".format(newusr['name'])
        new_users[ukey] = newusr

        # Add info to corresps dict keyed on new user id
        corresps[uid] = {
            'name': newusr['name'],
            'mail': newusr['mail']
        }

        for site in SITES:
            skey = "{}_uid".format(site)
            corresps[uid][skey] = urecs[site]['uid'] if site in urecs else 0

        compiled_users[ukey]['newuid'] = uid  # for writing correspondences later. No longer necessary?

    # Add rows to the new database using the new_users dictionary
    pout("Writing users to {}’s user table".format(outdbnm))
    truncatetable(outdbnm, 'users')  # Truncate and existing user info
    c = 0
    for name, rwdata in new_users.items():
        rkeys = []
        rvals = []
        for k, v in rwdata.items():
            # Sources has an extra user uuid field. Skip it.
            # TODO: Make sure this doesn't break the sources site. Maybe disable that module on Sources.
            if k == 'uuid':
                continue
            rkeys.append(k)
            rvals.append(v)

        doinsert(outdbnm, 'users', rkeys, rvals)
        c += 1

    # Adding back in the anonymous user (See http://drupal.org/node/1029506)
    insert_anon_qry = "INSERT INTO users (name, pass, mail, theme, signature, language, init, timezone) VALUES " \
                      "('', '', '', '', '', '', '', '')"
    doquery(outdbnm, insert_anon_qry, "commit")
    doquery(outdbnm, "UPDATE users SET uid = 0 WHERE name = ''", "commit")

    pout("{} users added to {} database".format(c, outdbnm), 2)

    # Create the uidcorresp table with correspondences
    pout("Creating the `uidcorresp` table with correspondences to old uids and new ones", 2)
    create_corresp_table()  # create the table in the db

    # Create the fields/column string (colstr) for the inserts
    # First sort the site names (Don't think I need to do this because SITES is a tuple not a set)
    sites_sorted = list(SITES)
    sites_sorted.sort()
    colstr = "(uid, name, mail, "
    for site in sites_sorted:
        # Add the column name
        colstr += "{}_uid, ".format(site)
    colstr += "created)"

    # Insert rows of corresponences in the same order as field names/columns above
    # by building a list and then inserting them all
    corrrowlist = []
    for guid, data in corresps.items():
        corresprow = [guid, data['name'], data['mail']]
        for site in sites_sorted:
            skey = "{}_uid".format(site)
            corresprow.append(data[skey])
        corresprow.append(int(time.time()))
        corrrowlist.append(corresprow)
    doinsertmany(SHARED_DB, 'uidcorresp', colstr, corrrowlist)


def create_corresp_table():
    """
    Creates the custom correspondence empty table in the SHARED_DB between users new global ID
    and their individual site IDs

    :return: None
    """
    qry = 'DROP TABLE IF EXISTS uidcorresp'  # Drop the table if it already exists
    doquery(SHARED_DB, qry, "commit")
    corresp_tbl_schema = "CREATE TABLE `uidcorresp` ( " \
                         "`uid` int(10) unsigned NOT NULL DEFAULT '0' COMMENT 'Primary Key: " \
                         "the New Unique user ID.', \n" \
                         "`name` varchar(60) NOT NULL DEFAULT '' COMMENT 'Unique user name.', \n" \
                         "`mail` varchar(254) DEFAULT '' COMMENT 'User’s e-mail address.', \n"
    for site in SITES:
        corresp_tbl_schema += "`{}_uid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT " \
                              "'Old {} UID',".format(site, (site.replace("_", " ")).capitalize())

    corresp_tbl_schema += "`created` int(11) NOT NULL DEFAULT '0' COMMENT 'Timestamp for when user was created', \n" \
                          "PRIMARY KEY (`uid`), \n" \
                          "UNIQUE KEY `name` (`name`), \n" \
                          "KEY `mail` (`mail`) \n" \
                          ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Stores user id correspondences " \
                          "from old IDS to new one'"
    doquery(SHARED_DB, corresp_tbl_schema, "commit")


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
        uidkey = "{}_uid".format(site)
        suid = uids[uidkey]
        qry = "SELECT * FROM authmap WHERE uid={}".format(suid)
        res = doquery(db, qry)
        qry = "select mail from users where uid={}".format(suid)
        email = doquery(db, qry, 'val')
        if res is not None and len(res) > 0:
            res = res[0]
            if res['module'] == DEFAULT_AUTH and 'virginia.edu' in email.lower():
                return True
            elif res['module'] == 'shib_auth' and 'virginia.edu' in email.lower():
                return True
    return False


def concat_authmap():
    """
    Concats authmap authorization tables into a single table in the default database
    :return: None
    """
    users = getallrows(SHARED_DB, 'users')
    pout("Truncated Shared Authmap table!", 2)
    truncatetable(SHARED_DB, 'authmap')
    for usr in users:
        if isauth(usr['uid']):
            doinsert(SHARED_DB, 'authmap', 'uid, authname, module', (usr['uid'], usr['name'], DEFAULT_AUTH))
    rows = getallrows(SHARED_DB, 'authmap')
    pout("Repopulated Shared Authmap table with {} rows".format(len(rows)), 2)


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
    pout("there are {} users".format(len(users)), 2)
    print("truncating current users_roles....")
    truncatetable(SHARED_DB, 'users_roles')
    ct = 0
    pout("determining new roles for each user...", 2)
    user_roles_rows = []
    for usr in users:
        ct += 1
        uid = usr['uid']
        corrs = getcorresps(uid)
        if uid == 429:
            pout("{} 429 corresps:".format(usr['name']), 2)
            pout(corrs, 2)
        if corrs is None:
            continue
        newrole = 1
        for site in SITES:
            key = "{}_uid".format(site)
            suid = corrs[key]
            qry = "SELECT rid FROM users_roles WHERE uid={}".format(suid)
            srole = doquery("{}{}".format(site, ENV), qry, 'val')
            if srole == 3:
                newrole = 3
                break
            if srole > 2:
                srole = translaterole(site, srole)
                if srole and srole > newrole:
                    newrole = srole
        if newrole > 2:
            user_roles_rows.append([uid, newrole])
    doinsertmany(SHARED_DB, 'users_roles', 'uid,rid', user_roles_rows)


def clean_up():
    """
    General clean up after the merging/update is complete
        1. truncate the autosaved_forms table in texts since this uses UID (rather than changing all of them)
    :return:
    """
    truncatetable('texts{}'.format(ENV), 'autosaved_forms')


def merge_all_tables(db=SHARED_DB):
    """
    This function will perform all the required tasks to merge the necessary tables to create shared users and enable
    Single Sign-on. Ultimately, it will be this function that will be run on Stage and Production

    :param db: str
        name of the global database (default to the shared db)
    :return: None
    """
    pout("Merging Users ....")
    merge_users(db)
    pout("Concatenating Authmap Table ....")
    concat_authmap()
    pout("Merging roles ...")
    merge_roles()
    pout("Concatenating Users Roles Table ....")
    concat_users_roles()
    clean_up()


if __name__ == '__main__':
    # pout("Merging all tables for PREDEV using the default db as the shared one.")
    merge_all_tables()

