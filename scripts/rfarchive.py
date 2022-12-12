import os.path
from subprocess import Popen, PIPE
import shutil
import pickle
from realfast import elastic


#Ids = elastic.get_ids('finalcands', datasetId='21B-169.sb40155499.eb41755976.59678.82444094907')
with open('finalcands_Ids.pkl', 'rb') as fp:  # faster
    Ids = pickle.load(fp)

try:
    with open('finished_Ids.pkl', 'rb') as fp:  # sdmnames already done
        finished = pickle.load(fp)
except FileNotFoundError:
    print("No finished pkl found. Starting anew")
    finished = []

Ids = elastic.get_ids('finalcands', caseyjlaw_tags="astrophysical,archive")

for i, Id in enumerate(Ids):
    source = elastic.get_doc('finalcands', Id)['_source']
    if 'sdmname' in source:
        sdmname = source['sdmname']
        if sdmname in finished:
            continue
        print(f"Working on Id {Id} with sdmname {sdmname}")
        p = Popen(["./rfarchive.sh", "create", sdmname], stdout=PIPE)
        p.wait()

        # parse p.stdout for "BDF not found" and "No SDM found", then ingest
        line = p.stdout.read().decode("utf8")
        if "BDF not found" not in line and "No SDM found" not in line and "No bdf found" not in line:
            p = Popen(["./rfarchive.sh", "ingest", sdmname], stdout=PIPE)
            p.wait()
        else:
            print("Not archiving if no SDM or BDF")

        line = p.stdout.read().decode("utf8")
        print(line)
        if os.path.exists(f"/users/claw/fasttransients/realfast/tmp/{sdmname}"):
            print("Cleaning up...")
            shutil.rmtree(f'/users/claw/fasttransients/realfast/tmp/{sdmname}')
        else:
            print("No SDM to clean up")

        finished.append(sdmname)
        # occasionally...
        if not i % 5:
            print("Updating finished_Ids.pkl")
            with open('finished_Ids.pkl', 'wb') as fp:
                pickle.dump(finished, fp)

        print()
        print()
