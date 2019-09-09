from common_func import *
import csv

tempdata = getallrows('images_predev', 'node')
imgnodes = {}
for imgn in tempdata:
    imgnodes[imgn['nid']] = imgn
badrows = []
fnm = '../data/images-bad-uid-conv.csv'
with open(fnm, 'r') as din:
    data = csv.DictReader(din)
    for rw in data:
        badrows.append(dict(rw))

types = []
missing = []
imgs = []
agents = []
for rw in badrows:
    nid = int(rw['nid'])
    if nid in imgnodes.keys():
        typ = imgnodes[nid]['type']
        if typ not in types:
            types.append(typ)
        if typ == 'shanti_image':
            imgs.append(imgnodes[nid])
        else:
            agents.append(imgnodes[nid])
    else:
        missing.append(nid)

# print(badrows[0:5])
kys = list(imgnodes.keys())
# print(kys[1], imgnodes[kys[1]])
print("{} missing nodes out of {}".format(len(missing), len(badrows)))

print(types)

print("{} are images".format(len(imgs)))

with open('../data/bad-uid-imgs.csv', 'w') as dataout:
    dict_write = csv.DictWriter(dataout, imgs[0].keys())
    dict_write.writeheader()
    dict_write.writerows(imgs)

with open('../data/bad-uid-agents.csv', 'w') as dataout:
    dict_write = csv.DictWriter(dataout, agents[0].keys())
    dict_write.writeheader()
    dict_write.writerows(agents)
