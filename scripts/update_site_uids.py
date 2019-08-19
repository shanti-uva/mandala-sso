"""
Scripts to update the uids on individual sites to use the global uids stored in the default site's db

Written: Than Grove
Date: Aug. 14, 2019

"""

from common_func import *


def update_uids_in_table(site, tblnm, idcol, ucol='uid'):
    db = "{}{}".format(site, ENV)
    pout("Updating {} in {}".format(tblnm, db), 2)
    rows = getallrows(db, tblnm)
    noguids = ["||uid||{}||".format(idcol)]
    for rw in rows:
        suid = rw[ucol]
        guid = find_uid(site, suid)
        if guid is not None:
            setstr = 'uid={}'.format(guid)
            condstr = '{}={}'.format(idcol, rw[idcol])
            update_single_col(db, tblnm, setstr, condstr)
        else:
            noguids.append("{}|{}".format(suid, rw[idcol]))

    outflnm = "../data/{}-orphaned-uids-{}.dat".format(db, tblnm)
    if len(noguids) > 1:
        pout("{} {} uids without global uid in {}. Writing info to {}".format(len(noguids) - 1, db, tblnm, outflnm))
        with open(outflnm, 'w') as dout:
            for nogid in noguids:
                dout.write("{}\n".format(nogid))
    else:
        pout("All uids had global uid")


def update_all_tables_with_uids(site):
    update_uids_in_table(site, 'node', 'nid', 'uid')
    update_uids_in_table(site, 'node_revision', 'nid', 'uid')
    update_uids_in_table(site, 'file_managed', 'fid', 'uid')
    update_uids_in_table(site, 'og_users_roles', 'gid', 'uid')


def truncate_all_tables_to_repopulate(site):
    db = "{}{}".format(site, ENV)
    tbls = [
        'autosaved_forms',
        'history',
        'password_policy_history',
        'realname'
    ]
    for tbl in tbls:
        truncatetable(db, tbl)


if __name__ == "__main__":
    asite = 'visuals'
    print("Doing {}".format(asite))
    update_all_tables_with_uids(asite)
    truncate_all_tables_to_repopulate(asite)
    print("Done")

