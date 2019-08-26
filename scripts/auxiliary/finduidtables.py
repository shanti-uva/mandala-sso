

import mysql.connector

SITES = ('audio_video', 'images', 'mandala', 'texts', 'sources', 'visuals')
ENV = '_predev'

uidtables = {}
userentitytables = []
for site in SITES:
    print("\nDoing {}".format(site))
    userentitytables.append("SITE {}".format(site))
    db = "{}{}".format(site, ENV)
    cnx = mysql.connector.connect(user='root', port=33067, database=db)
    cursor = cnx.cursor()
    cursor.execute('SHOW TABLES')
    tables = list(cursor)
    tables = [tables[n][0] for n in range(0, len(tables))]
    for table in tables:
        if table.startswith('__'):
            continue
        print("\rTable: {}           ".format(table), end='')
        cursor.execute('SHOW COLUMNS IN {}'.format(table))
        fields = list(cursor)
        fields = [fields[n][0] for n in range(0, len(fields))]
        for fld in fields:
            if 'uid' in fld and not 'tcuid' in fld:
                cursor.execute('SELECT count(*) FROM {}'.format(table))
                cnt = (cursor.fetchone())[0]
                if cnt > 0:
                    if table in uidtables:
                        uidtables[table]['sites'].add(site)
                        uidtables[table]['fields'].add(fld)
                    else:
                        uidtables[table] = {
                            'sites': {site},
                            'fields': {fld}
                        }
            elif fld == 'entity_type':
                cursor.execute('SELECT count(*) FROM {} WHERE entity_type=\'user\''.format(table))
                cnt = (cursor.fetchone())[0]
                if cnt > 0:
                    userentitytables.append("{}".format(table))

print("Done Tabulating")
print("There are {} unique tables with uid fields".format(len(uidtables)))
tbls = list(uidtables.keys())
tbls.sort()
for tbl in tbls:
    tdata = uidtables.get(tbl)
    print("{}:".format(tbl))
    sites = list(tdata['sites'])
    sites.sort()
    print("\tsites: {}".format(', '.join(sites)))
    flds = list(tdata['fields'])
    flds.sort()
    print("\tfields: {}".format(', '.join(flds)))


print("userentitytables!")
for ln in userentitytables:
    print(ln)
