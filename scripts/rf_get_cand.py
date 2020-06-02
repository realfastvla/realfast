#! /usr/bin/env python
import sys
import os
import json
import astropy.coordinates
import astropy.units as u
import sdmpy
from elasticsearch import Elasticsearch

# This script takes a realfast portal candidate ID, and 
# assembles the SDM, BDF and PNG files into the current
# directory.  It then modifies the SDM Annotation table
# to contain correct metadata about the candidate, as
# retrieved from the portal.

try:
    candid = sys.argv[1]
except IndexError:
    print("Usage: rf_get_cand.py candId")
    sys.exit(1)

es = Elasticsearch('http://realfast.nrao.edu:9200/')
rfidx = 'new'

# Get the cand from portal
q = {'query':{
        'match':{'candId':candid},
    }}
r = es.search(index=rfidx+'cands',body=q,size=1)
try:
    cand = r['hits']['hits'][0]['_source']
except IndexError:
    raise(RuntimeError,"No matching candId")
noiseid= '%s.%d.*' % (cand['scanId'], cand['segment'])
#print(cand)

# Get the noise values from portal
q = {'query':{
        'wildcard':{'_id':noiseid}
    }}
rn = es.search(index=rfidx+'noises',body=q,size=1)
try:
    noise = rn['hits']['hits'][0]['_source']
except IndexError:
    raise(RuntimeError,"No noise entry found")
#print(noise)

print('SDM: %s' % (cand['sdmname']))
print('PNG: %s' % (cand['png_url']))

# Get the SDM from MCAF dir
sdmname = cand['sdmname']
mcafdir = '/home/mctest/evla/mcaf/workspace'
if not os.path.exists(sdmname):
    os.system('rsync -aqP %s/%s .' % (mcafdir,sdmname))

# Get the BDF from CBE lustre
bdfdir = '/lustre/evla/wcbe/data/realfast'
sdm = sdmpy.SDM(sdmname,bdfdir=bdfdir,use_xsd=False)
scan = sdm.scan(1)
bdfpath = os.path.join(sdmname,'ASDMBinary')
try:
    os.mkdir(bdfpath)
except OSError:
    pass
rsync_cmd = "rsync -aq cbe-master:%s %s" % (scan.bdf_fname, bdfpath)
print(rsync_cmd)
os.system(rsync_cmd)

# Get the PNG from web
os.system("wget -q -N %s" % (cand['png_url']))

# Fix up Annotation values in SDM
ann = json.loads(str(sdm['Annotation'][0].sValue))
if cand['frbprob']>0.9:
    ann['rf_QA_label'] = 'Good'
elif cand['frbprob']>0.5:
    ann['rf_QA_label'] = 'Marginal'
elif cand['frbprob']>0.0:
    ann['rf_QA_label'] = 'Questionable'
else:
    ann['rf_QA_label'] = 'None'
ann['rf_QA_zero_fraction'] = noise['zerofrac']
ann['rf_QA_visibility_noise'] = noise['noiseperbl']
ann['rf_QA_image_noise'] = noise['imstd']
# Fix busted RA and dec
if ann['transient_RA']=='[':
    c = astropy.coordinates.SkyCoord(cand['ra']*u.deg, cand['dec']*u.deg)
    ann['transient_RA'] = c.ra.to_string(unit=u.hourangle,sep=':',pad=True)
    ann['transient_Dec'] = c.dec.to_string(unit=u.deg,sep=':',pad=True,alwayssign=True)
# Fix units on RA error
ann['transient_RA_error'] = ann['transient_Dec_error'] / 15.0
sdm['Annotation'][0].sValue = json.dumps(ann)

# Write out updated SDM
sdm.write(sdmname)
