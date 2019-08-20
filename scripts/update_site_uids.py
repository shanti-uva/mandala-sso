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
    fails = []
    tries = 0
    # Some tables, e.g. og_user_groups, use indexes that incorporate user id
    # When changing user ids, sometimes there are conflicts between these unique indexes
    # Iterate over rows, capture failures, and try again (Actually don't need this, but leaving for now!)
    while tries == 0 or len(fails) > 0:
        faillist = []
        tries += 1
        for rw in rows:
            suid = rw[ucol]
            guid = find_uid(site, suid)
            if guid is not None:
                setstr = 'uid={}'.format(guid)
                condstr = '{}={}'.format(idcol, rw[idcol])
                try:
                    update_single_col(db, tblnm, setstr, condstr, True)
                except mysql.connector.Error as err:
                    print("Update failed:\n\tset: {} \n\tcond: {}".format(setstr, condstr))
                    faillist.append(rw)
            else:
                noguids.append("{}|{}".format(suid, rw[idcol]))
        if len(faillist) > 0:
            if tries == 10:
                pout("Warning!!! Aborting after 10 tries. Still {} failed rows".format(len(fails)), 2)
                print(faillist)
                break
            else:
                pout("{} rows failed to update. Doing try {}".format(len(fails), tries + 1))
                rows = faillist.copy()
                fails = faillist.copy()
        else:
            fails = []

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
    update_uids_in_table(site, 'og_users_roles', 'uid', 'uid')


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

