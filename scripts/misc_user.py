from os import system, getcwd, remove
from common_func import *

def load_uids():
    myuids = []
    with open('usrs_noname.dat', 'r') as datain:
        for ln in datain:
            ln = ln.strip()
            if not isinstance(ln, int) and ln.isnumeric:
                ln = int(ln)
            myuids.append(ln)

    myuids.sort()
    return myuids


def load_unames():
    myunames = []
    with open('uvaids-nonames.dat', 'r') as datain:
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


if __name__ == "__main__":
    # Load uids from usrs_noname.dat and write uvaids with uid to uvaids-nonames.dat
    # uids = load_uids()
    # with open('uvaids-nonames.dat', 'w') as outfile:
    #     for uid in uids:
    #         unm = get_user_name(uid)
    #         outfile.write("{}|{}\n".format(uid, unm))
    #
    # print("{}".format(len(uids)))

    unms = load_unames()
    gufnms = {}
    ct = 0
    for unmstr in unms:
        if ct > 10:
            break
        guid, uvanm = unmstr.split('|')
        firstnm, lastnm = get_full_name('uid', uvanm)
        if firstnm is not None:
            print(firstnm, lastnm)
            ct += 1
            if ct > 10:
                exit(0)

        if lastnm is not None:
            add_name_field('last', guid, lastnm)

        if firstnm is not None:
            add_name_field('first', guid, firstnm)