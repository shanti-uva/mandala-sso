'''
Script to be run *after* create_shared_tables.py which creates a single user table. This script repopulate individual
site tables that have a UID field with the new global UID of the same person.

The function takes 1. the site name = db name (e.g. 'visuals') and 2. the table name

'''


from common_func import *


def update_uid_in_table(asite, atable):
    tblrows = getallrows(asite, atable)
    auid = 1
    corresps = getcorrespsbysite(asite, auid)


if __name__ == '__main__':

    site = 'visuals'
    tbl = 'node'


    uid_tables = [
        'file_managed',
        'flagging',
        'history',
        'node',
        'node_revision',
        'og_users_roles',
        'profile_value',
        'realname',
        'redirect',
        'shanti_images',
        'shivadata_links',
        'votingapi_vote'
    ]

    print("Updating user ids in {}: ".format(db))
    for tbl in uid_tables:
        if tableexists(site, tbl):

            print("{} updated!".format(tbl))
        else:
            print("{} does NOT exists".format(tbl))

        # update_uid_in_table(site, tbl)