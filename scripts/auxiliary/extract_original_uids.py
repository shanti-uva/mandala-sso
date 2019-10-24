from os import system, getcwd, remove
from common_func import *
import json


def extract_uids(mydb, mytbl, idflds):
    qry = 'SELECT {} FROM {}'.format(', '.join(idflds), mytbl)
    res = doquery(mydb, qry)
    return res


def write_json(reslist, flnm):
    '''
    Write a list of rows from a MYSQL query into a file as JSON

    :param reslist: list
        A list of mysql rows (as dictionaries)
    :param flnm: str
        The filename where the outputed JSON will be written
    :return: None
    '''
    outfl = '/Users/thangrove/Documents/Sandbox/Test/sso/data/original_uids/{}'.format(flnm)
    with open(outfl, 'w') as fileout:
        json.dump(reslist, fileout, sort_keys=True, indent=4)


if __name__ == "__main__":
    dbs = ("av_old_tables", "images_old_tables", "mandala_old_tables",
           "sources_old_tables", "texts_old_tables", "visuals_old_tables")

    tbls = {
        "node": ('uid', 'nid', 'title', 'type'),
        "node_revision": ('uid', 'nid', 'title'),
        "file_managed": ('uid', 'fid', 'filename'),
        "og_membership": ('id', 'etid', 'entity_type', 'field_name'),
        "og_users_roles": ('uid', 'rid', 'gid')
    }

    for db in dbs:
        dbnm = db.replace('_old_tables', '')
        print("Doing {}".format(dbnm))
        for tblnm, flds in tbls.items():
            print("\tExporting {}".format(tblnm))
            uidlist = extract_uids(db, tblnm, flds)
            outflnm = "{}-{}-uids.json".format(dbnm, tblnm.replace('_', '-'))
            write_json(uidlist, outflnm)
