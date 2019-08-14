from os import system, getcwd, remove
from common_func import *


def add_name_field(tbl, uid, nms):
    """
    Insert a first or last name field in the global user name fields
    First and last name tables have the following columns:
        'entity_type', 'bundle', 'deleted', 'entity_id', 'revision_id', 'language', 'delta', 'field_name_value', 'field_name_format'

    :param tbl:
    :param uid:
    :param nms:
    :return:
    """
    base_cols = ('entity_type', 'bundle', 'deleted', 'entity_id', 'revision_id', 'language', 'delta')
    fnm = tbl.replace('field_data_', '').replace('field_revision_', '')
    cols = base_cols + ("{}_value".format(fnm), "{}_format".format(fnm))
    ind = 0 if 'first' in fnm else 1
    vals = ('user', 'user', '0', str(uid), str(uid), 'und', '0', nms[ind], None)
    doinsert(SHARED_DB, tbl, cols, vals)


def combine_realnames():
    """
    Need to do this....
    :return:
    """
    pass


def do_uid_full_names():
    """

    :return:
    """
    unms = []
    with open('../data/uvaids_no_name.dat', 'r') as datain:
        for ln in datain:
            ln = ln.strip()
            unms.append(ln)

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


def get_full_name(fld, val):
    """
    Use LDAP call to UVA to find out user's first and last name from their UVA computing ID

    :param fld:
    :param val:
    :return:
    """
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
    """
    Load guids of users with no first or last name from ../data/guids_no_name.dat
    and write uvaids with uid to uvaids-nonames.dat

    :return:
    """
    #
    uids = []
    with open('../data/guids_no_name.dat', 'r') as datain:
        for ln in datain:
            ln = ln.strip()
            if not isinstance(ln, int) and ln.isnumeric:
                ln = int(ln)
            uids.append(ln)
    uids.sort()

    with open('../data/uvaids_no_name.dat', 'w') as outfile:
        for uid in uids:
            usr = loaduser(uid)
            outfile.write("{}|{}\n".format(uid, usr['name']))
    print("Wrote {} guids with UVA IDs to ../data/uvaids_no_name.dat".format(len(uids)))


def get_user_names(guid):
    """
    For a single global user, iterate through the sites and using the uid correspondences find out if they have an
    entry for first and last name on one of the sites.

    :param guid:
    :return: mixed
        There are three possibilites to be returned:
            1. a tuple of (first name, last name)
            2. False if nothing found
            3. a dictionary keyed on site name containing tuples of (first name, last name)
    """
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
                results[site] = (fname,  lname)
    fname = ''
    lname = ''
    myct = 0
    is_diff = False
    for site, nmpts in results.items():
        if myct == 0:
            fname = nmpts[0]
            lname = nmpts[1]
        else:
            if fname != nmpts[0] or lname != nmpts[1]:
                is_diff = True
        myct += 1

    if is_diff:
        return results
    elif fname == '' and lname == '':
        return False
    else:
        return fname, lname


def merge_user_names():
    """
    Create a merged table of users first and last names to use on all sites
    Need to create these fields on the default site: field_first_name and field_last_name
    Writes out a file ../data/guids_no_name.dat with a list of guids that do not have a fn and ln

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
        if int(guid) == 0:
            continue
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
            row = (guid, nmtup[0], nmtup[1])
        if row is None:
            notfound.append(int(guid))

        if row is not None:
            # Add row to all the name tables
            for tbl in name_tables:
                add_name_field(tbl, row[0], row[1:])
            insct += 1

    print("\n")
    pout("{} name fields inserted".format(insct), 2)

    notfound.sort()
    with open('../data/guids_no_name.dat', 'w') as dataout:
        for nf in notfound:
            dataout.write("{}\n".format(nf))


if __name__ == "__main__":
    # get_noname_uids()
    merge_user_names()

    print("done!")
