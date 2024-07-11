import os.path
from subprocess import Popen, PIPE
import shutil
import pickle
from realfast import elastic
from time import sleep
from astropy.time import Time

# get all Ids
Ids = elastic.get_ids('finalcands', caseyjlaw_tags="astrophysical,archive")

# any project code filters can be added here
# Ids = [Id for Id in Ids if <code string> in Id]

# filter out most recent
mjds = [int(Id.split('.eb')[1].split('.')[1]) if '.eb' in Id else 0 for Id in Ids]
now = Time.now().mjd
Ids = [Id for i, Id in enumerate(Ids) if mjds[i] < now-14]  # archived after 14 days

try:
    with open('finished_Ids.pkl', 'rb') as fp:  # sdmnames already done
        finished = pickle.load(fp)
except FileNotFoundError:
    print("No finished pkl found. Starting anew")
    finished = []

raningest = False
for i, Id in enumerate(Ids):
    source = elastic.get_doc('finalcands', Id)['_source']
    if 'sdmname' in source:
        sdmname = source['sdmname']
        if sdmname in finished:
            print(f"Skipping ingested file: {sdmname}")
            continue
        print(f"Working on Id {Id} with sdmname {sdmname}")
#        p = Popen(["./rfarchive.sh", "create", sdmname], stdout=PIPE)
        p = Popen(["./archive.sh", sdmname, "dsoc-prod"], stdout=PIPE, stderr=PIPE)
        p.wait()

        # parse p.stdout for "BDF not found" and "No SDM found", then ingest
        stdout = p.stdout.read().decode("utf8")
        stderr = p.stderr.read().decode("utf8")
#        if "BDF not found" not in stderr and "No SDM found" not in stderr and "No bdf found" not in stderr:
        if "No SDM found" not in stderr:
#            p = Popen(["./rfarchive.sh", "ingest", sdmname], stdout=PIPE)
#            p.wait()
            raningest = True
        else:
            raningest = False
            print("Not archiving if no SDM or BDF")

#        line = p.stdout.read().decode("utf8")
#        print(line)
#        if os.path.exists(f"/users/claw/fasttransients/realfast/tmp/{sdmname}"):
#            print("Cleaning up...")
#            shutil.rmtree(f'/users/claw/fasttransients/realfast/tmp/{sdmname}')
#        else:
#            print("No SDM to clean up")

        finished.append(sdmname)
        # occasionally...
        if not i % 5:
            print("Updating finished_Ids.pkl")
            with open('finished_Ids.pkl', 'wb') as fp:
                pickle.dump(finished, fp)

        if raningest:
            print('Ran ingest. Sleep for 10 seconds...')
            sleep(10)
        print()
        print()
