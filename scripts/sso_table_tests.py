from common_func import *
import pandas as pd
import csv


def verify_one_corresp(uid, rettype='boolean'):
    found = False
    corresps = getcorresps(uid)
    cname = corresps['name']
    if rettype == 'print':
        print("global:  {}, {}".format(cname, uid))
    for site in SITES:
        ckey = "{}_uid".format(site)
        cuid = corresps[ckey]
        res = 'n/a'
        if cuid > 0:
            surs = loaduser(cuid, site)
            if surs is not None:
                if surs['name'] == cname:
                    res = "Found and matched"
                    found = True
                else:
                    res = "Name mismatch: {} ≠ {}".format(surs['name'], cname)
                    surs = loaduser(cname, site)
                    if surs is not None:
                        res += " — Corred UID: {}".format(surs['uid'])
            else:
                surs = loaduser(cname, site)
                if surs is not None:
                    res = "User ID not found — Corred UID: {}".format(surs['uid'])
                else:
                    res = "User is not in {} DB".format(site)
        if rettype == 'print':
            print("{}:\t{}\t{}".format(site, cuid, res))
        else:
            return found


def verify_user_in_site(gusr, site):
    guid = gusr['uid']
    gnm = gusr['name']
    corrs = getcorresps(guid)
    if corrs is None:
        return True  # so it doesn't print another error message

    suid = corrs["{}_uid".format(site)]
    if suid == 0:
        surs = loaduser(gnm, site)
        if surs is not None and surs['uid'] != suid:
            return False
    else:
        surs = loaduser(suid, site)
        if surs is None or surs['name'] != gnm:
            return False
    return True


def verify_all_corresp():
    users = getallrows(SHARED_DB, 'users')
    ct = 0
    bad = 0
    for usr in users:
        ct += 1
        for site in SITES:
            if not verify_user_in_site(usr, site):
                corrsp = getcorresps(usr['uid'])
                suid = "{}_uid".format(site)
                surs = loaduser(corrsp[suid], site)
                print("{}, {}, {} ({})".format(usr['uid'], usr['name'], site, suid))
                if surs is not None:
                    print("\t{}:{}".format(surs['uid'], surs['name']))
                print("\t{}".format(corrsp))
                bad += 1

    print("\nTotal: {}\nBad: {}".format(ct, bad))


def showuserinfo(uid):
    from pprint import PrettyPrinter
    pp = PrettyPrinter(indent=2)
    uinfo = loadalluserinfo(uid)
    pp.pprint(uinfo)


def check_usr_authmap_across_sites(uid):
    uinfo = loadalluserinfo(uid)
    print("{} ({})\t{}".format(uinfo['name'], uinfo['uid'], uinfo['mail']))
    for site in SITES:
        sukey = "{}_user".format(site)
        if sukey in uinfo:
            susr = uinfo[sukey]
            if susr is not None and susr['uid'] > 0:
                am = doquery(
                    "{}{}".format(site, ENV),
                    "SELECT module FROM authmap WHERE uid={}".format(susr['uid']),
                    "val"
                )
                print("{} ({})\t{}\t{}".format(susr['name'], susr['uid'], susr['mail'], am))


def find_email_globally(emailpt):
    for site in SITES:
        db = "{}{}".format(site, ENV)
        qry = "SELECT * FROM users WHERE mail LIKE '%{}%'".format(emailpt)
        # print(db, qry)
        res = doquery(db, qry)
        if res is not None:
            print(site, res)


def check_uids_in_table(site, tbl, indcol, outfile=False):
    print("\tChecking {}".format(tbl))
    db = "{}{}".format(site, ENV)
    tmpcorresps = loadcorresps()
    corresps = {}
    for ind, rw in tmpcorresps.items():
        siteuid = rw["{}_uid".format(site)]
        corresps[siteuid] = rw

    tempnewdata = getallrows(db, tbl)
    newdata = {}
    for rw in tempnewdata:
        newdata[rw[indcol]] = rw

    sitestr = 'av' if site == 'audio_video' else site
    olddb = "{}_old_tables".format(sitestr)
    tempolddata = getallrows(olddb, tbl)
    olddata = {}
    for rw in tempolddata:
        olddata[rw[indcol]] = rw

    badlist = []
    for ind, rw in olddata.items():
        olduid = rw['uid']
        newuid = newdata[ind]['uid']
        corruid = corresps[olduid]['uid']

        if newuid != corruid:
            info = {
                indcol: ind,
                'corruid': corruid,
                'olduid': olduid,
                'newuid': newuid
            }
            badlist.append(info)

    print("{} bad rows out of {}".format(len(badlist), len(olddata)))

    if len(badlist) > 0:
        if outfile:
            with open(outfile, 'w') as outf:
                keys = badlist[0].keys()
                dict_write = csv.DictWriter(outf, keys)
                dict_write.writeheader()
                dict_write.writerows(badlist)
        else:
            df = pd.DataFrame(badlist)
            with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                print(df)


if __name__ == '__main__':
    imglistfile = '../data/images-bad-uid-conv.csv'
    print("Doing just images to file: {}".format(imglistfile))
    check_uids_in_table('images', 'node', 'nid', imglistfile)

    # for site in SITES:
    #     print("Doing {} Site...".format(site))
    #     check_uids_in_table(site, 'node', 'nid')
    #     check_uids_in_table(site, 'node_revision', 'nid')
    #     # check_uids_in_table(site, 'file_managed', 'fid') # Doesn't work with file managed

    # find_email_globally('vck6mg')
    # corrs = getcorrespsbysite('audio_video', 312)
    # print(corrs)
    # verify_all_corresp()
