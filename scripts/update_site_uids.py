"""
Scripts to update the uids on individual sites to use the global uids stored in the default site's db

Written: Than Grove
Date: Aug. 14, 2019

"""

from common_func import *


def update_uids_in_table(site, tblnm, idcol, ucol='uid'):
    db = "{}{}".format(site, ENV)
    pout("Updating {} in {}".format(tblnm, db), 2)
    log = open('../logs/{}-{}-uid-update-{}.log'.format(site, tblnm, int(time.time())), 'w')
    rows = getallrows(db, tblnm)
    noguids = ["||uid||{}||".format(idcol)]
    fails = []
    tries = 0
    # Some tables, e.g. og_user_groups, use indexes that incorporate user id
    # When changing user ids, sometimes there are conflicts between these unique indexes
    # Iterate over rows, capture failures, and try again (Actually don't need this, but leaving for now!)
    while tries == 0:  # or len(fails) > 0:
        faillist = []
        tries += 1
        pout("Doing {} rows".format(len(rows)), 3)
        print("\t\t\t", end="")
        rct = 0
        update_list = []
        for rw in rows:
            rct += 1
            suid = rw[ucol]
            guid = find_uid(site, suid)
            if guid is not None:
                setstr = 'uid={}'.format(guid)
                condstr = '{}={}'.format(idcol, rw[idcol])
                try:
                    if rct % 1000 != 0:
                        update_list.append((guid, rw[idcol]))
                    else:
                        update_single_col_many(db, tblnm, 'uid', idcol, update_list, True)
                        update_list = []
                        # time.sleep(.001)
                    # update_single_col(db, tblnm, setstr, condstr, True)
                    # time.sleep(.001)  # Pausing to let mysql catch its breath
                except mysql.connector.Error as err:
                    log.write("Update failed:\tset: {} \tcond: {} \n".format(setstr, condstr))
                    log.write(str(err))
                    faillist = faillist + update_list
                    # faillist.append(rw)
                print("\r\t\t\tRow {}          ".format(rct), end="")
            else:
                noguids.append("{}|{}".format(suid, rw[idcol]))
        print(" ")  # End same line output
        if len(faillist) > 0:
            if tries == 10:
                pout("Warning!!! Aborting after 10 tries. Still {} failed rows".format(len(faillist)), 2)
                log.write("Aborting after 10 tries. Still {} failed rows".format(len(faillist)))
                log.write(str(faillist))
                # print(faillist)
                break
            elif tries == 1:
                pout("Warning!!! {} rows failed.".format(len(faillist)), 2)
                log.write("{} rows failed".format(len(faillist)))
                log.write(str(faillist))
            else:
                pout("{} rows failed to update. Doing try {}".format(len(faillist), tries + 1))
                rows = faillist.copy()
                fails = faillist.copy()
        else:
            fails = []

    outflnm = "../data/{}-orphaned-uids-{}.dat".format(db, tblnm)
    if len(noguids) > 1:
        pout("{} {} uids without global uid in {}. Writing info to {}".format(len(noguids) - 1, db, tblnm, outflnm), 3)
        with open(outflnm, 'w') as dout:
            for nogid in noguids:
                dout.write("{}\n".format(nogid))
    else:
        pout("All uids had a global uid", 3)


def replace_uids_in_table(site, tblnm, ucol='uid', is_entity=False):
    """
    Some tables with aggregated primary indices such as og_users_roles need to be read in, truncated, and rows
    added back with UID replaced to avoid primary index conflicts which occur when normally updating

    :param site:
    :param tblnm:
    :param ucol:
    :return:
    """
    db = "{}{}".format(site, ENV)
    pout("Updating {} in {}".format(tblnm, db), 2)
    # log = open('../logs/{}-{}-uid-update-{}.log'.format(site, tblnm, int(time.time())), 'w')
    colnms = doquery(db, 'SHOW columns FROM {}'.format(tblnm), 'list')
    rows = doquery(db, 'SELECT * from {} where entity_type="user"'.format(tblnm)) \
        if is_entity else getallrows(db, tblnm)
    pout("{} rows in...".format(len(rows)), 3)
    rowsout = []
    if len(rows) > 0:
        if is_entity:
            doquery(db, 'DELETE FROM {} WHERE entity_type="user"'.format(tblnm), 'commit')
        else:
            truncatetable(db, tblnm)
        for rw in rows:
            suid = rw[ucol]
            guid = find_uid(site, suid)
            if guid and guid > 0:
                rw[ucol] = guid
                outrow = []
                for cnm in colnms:
                    outrow.append(rw[cnm])
                outrow = tuple(outrow)
                rowsout.append(outrow)
            else:
                pout("tbl: {}, uid: {} has no guid".format(tblnm, suid), 3)
        doinsertmany(db, tblnm, colnms, rowsout)
        pout("{} rows updated".format(len(rowsout)), 3)


def update_user_entity_id_columns(site, tblnm, idcol, uidcol='etid'):
    db = "{}{}".format(site, ENV)
    pout("Updating user entity ids for table {} in {}".format(tblnm, db), 2)
    rows = doquery(db, 'SELECT * FROM {} WHERE entity_type="user"'.format(tblnm))
    noguids = ["||uid||{}||".format(idcol)]
    log = open('../logs/{}-{}-ueid-update-{}.log'.format(site, tblnm, int(time.time())), 'w')
    pout("Doing {} rows for user entities".format(len(rows)), 3)
    print("\t\t\t", end="")
    rct = 0
    for rw in rows:
        rct += 1
        suid = rw[uidcol]
        guid = find_uid(site, suid)
        if guid is not None:
            setstr = '{}={}'.format(uidcol, guid)
            condstr = '{}={}'.format(idcol, rw[idcol])
            try:
                update_single_col(db, tblnm, setstr, condstr, True)
                time.sleep(.001)  # Pausing to let mysql catch its breath
            except mysql.connector.Error as err:
                log.write("Update failed:\tset: {} \tcond: {} \n".format(setstr, condstr))

            print("\r\t\t\tRow {}          ".format(rct), end="")
        else:
            noguids.append("{}|{}".format(suid, rw[idcol]))
    print(" ")  # End same line output

    outflnm = "../data/{}-orphaned-uids-{}.dat".format(db, tblnm)
    if len(noguids) > 1:
        pout("{} {} uids without global uid in {}. Writing info to {}".format(len(noguids) - 1, db, tblnm, outflnm), 3)
        with open(outflnm, 'w') as dout:
            for nogid in noguids:
                dout.write("{}\n".format(nogid))
    else:
        pout("All uids had a global uid", 3)


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
    for asite in SITES:
        print("Doing {}".format(asite))
        db = "{}{}".format(asite, ENV)
        update_uids_in_table(asite, 'node', 'nid', 'uid')
        update_uids_in_table(asite, 'node_revision', 'nid', 'uid')
        update_uids_in_table(asite, 'file_managed', 'fid', 'uid')
        if tableexists(db, 'og_users_roles'):
            replace_uids_in_table(asite, 'og_users_roles', 'uid')
        if tableexists(db, 'og_membership'):
            update_user_entity_id_columns(asite, 'og_membership', 'id', 'etid')
        if tableexists(db, 'views_natural_sort'):
            replace_uids_in_table(asite, 'views_natural_sort', 'eid', True)

    print("Done")

