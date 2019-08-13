from os import system, getcwd, remove
from common_func import *


def load_uids():
    myuids = []
    with open('../data/guids_no_name.dat', 'r') as datain:
        for ln in datain:
            ln = ln.strip()
            if not isinstance(ln, int) and ln.isnumeric:
                ln = int(ln)
            myuids.append(ln)

    myuids.sort()
    return myuids


def load_unames():
    myunames = []
    with open('../data/uvaids-nonames.dat', 'r') as datain:
        for ln in datain:
            ln = ln.strip()
            myunames.append(ln)

    myunames.sort()
    return myunames


def get_user_name(auid):
    usr = loaduser(auid)
    return usr['name']


def get_full_name(fld, val):
    tmpfile = '../tempout.tmp'
    ldap_cmd = 'ldapsearch -LLL -b "ou=People,o=University of Virginia,c=US" -h ldap.virginia.edu -x ' \
               '"({}={})" givenName sn >../tempout.tmp'.format(fld.strip(), val.strip())
    system(ldap_cmd)
    fnm = None
    lnm = None
    with open(tmpfile, 'r') as tmpin:
        for ln in tmpin:
            if 'givenName:' in ln:
                fnm = ln.replace('givenName:', '').strip()
            elif 'sn:' in ln:
                lnm = ln.replace('sn:', '').strip()

    remove(tmpfile)
    return fnm, lnm


def get_noname_uids():
    # Load uids from usrs_noname.dat and write uvaids with uid to uvaids-nonames.dat
    uids = load_uids()
    with open('../data/uvaids-nonames.dat', 'w') as outfile:
        for uid in uids:
            unm = get_user_name(uid)
            outfile.write("{}|{}\n".format(uid, unm))

    print("{}".format(len(uids)))


def merge_user_names():
    """
    Create a merged table of users first and last names to use on all sites
    Need to create these fields on the default site: field_first_name and field_last_name

    :return:
    """
    pout("Merging user name field values into fields on default site \n\t\t", 2)
    name_tables = (
        'field_data_field_first_name',
        'field_data_field_last_name',
        'field_revision_field_first_name',
        'field_revision_field_last_name'
    )
    guids = getalluids()
    ct = 0
    insct = 0
    notfound = []
    for guid in guids:
        ct += 1
        if ct % 100 == 0:
            pref = "\t" if ct == 100 else ""
            print("{}{} ... ".format(pref, ct), end='')
        unm = get_user_names(guid)
        if not unm or len(unm) < 2:
            notfound.append(guid)
            continue

        row = None
        if isinstance(unm, tuple):
            row = (guid, unm[0], unm[1])
        elif isinstance(unm, dict):
            ky1 = list(unm.keys())[0]
            nmtup = unm[ky1]
            row = (guid, nmtup['first'], nmtup['last'])
        if row is None:
            notfound.append(guid)

        if row is not None:
            # Row to first name table
            # first and last name tables have
            # 'user','user','0','151','151','und','0','Raf',NULL
            for tbl in name_tables:
                add_name_field(tbl, row[0], row[1:])
            insct += 1

    print("\n")
    pout("{} name fields inserted".format(insct), 2)

    print("Users Without Name:")
    return notfound


def add_name_field(tbl, uid, nms):
    base_cols = ('entity_type', 'bundle', 'deleted', 'entity_id', 'revision_id', 'language', 'delta')
    fnm = tbl.replace('field_data_', '').replace('field_revision_', '')
    cols = base_cols + ("{}_value".format(fnm), "{}_format".format(fnm))
    ind = 0 if 'first' in fnm else 1
    vals = ('user', 'user', '0', str(uid), str(uid), 'und', '0', nms[ind], None)
    doinsert(SHARED_DB, tbl, cols, vals)


def get_user_names(guid):
    results = {}
    uidcorrs = getcorresps(guid)
    if not uidcorrs:
        return False
    for site in SITES:
        if site in ('audio_video', 'mandala'):
            continue
        uid = uidcorrs["{}_uid".format(site)]
        if uid > 0:
            field_name = 'fname' if site in ('images', 'visuals') else 'first_name'
            qry = 'SELECT field_{0}_value FROM field_data_field_{0} WHERE entity_id={1}'.format(field_name, uid)
            fname = doquery("{}{}".format(site, ENV), qry, 'val')
            field_name = 'lname' if site in ('images', 'visuals') else 'last_name'
            qry = 'SELECT field_{0}_value FROM field_data_field_{0} WHERE entity_id={1}'.format(field_name, uid)
            lname = doquery("{}{}".format(site, ENV), qry, 'val')
            if lname:
                results[site] = {'first': fname, 'last': lname}
    fname = ''
    lname = ''
    myct = 0
    is_diff = False
    for site, nmpts in results.items():
        if myct == 0:
            fname = nmpts['first']
            lname = nmpts['last']
        else:
            if fname != nmpts['first'] or lname != nmpts['last']:
                is_diff = True
        myct += 1

    if is_diff:
        return results
    elif fname == '' and lname == '':
        return False
    else:
        return fname, lname


def combine_realnames():
    """
    Need to do this....
    :return:
    """
    pass



def do_uid_full_names():
    unms = load_unames()
    uvunms = []
    ct = 0
    print("Finding usr names: ")
    for unmstr in unms:
        guid, uvanm = unmstr.split('|')
        firstnm, lastnm = get_full_name('uid', uvanm)
        if firstnm is not None:
            uvunms.append("{} {}".format(firstnm, lastnm))
            print(".", end="")
            ct += 1
            if ct % 50 == 0:
                print(" ")

    print("\n")
    print("{} names found".format(len(uvunms)))



if __name__ == "__main__":
    # get_noname_uids()
    notfound = merge_user_names()
    notfound = [int(nfid) for nfid in notfound]
    notfound.sort()
    with open('../data/guids_no_name.dat', 'w') as dataout:
        for nf in notfound:
            dataout.wrtie(nf)
    print("done!")
