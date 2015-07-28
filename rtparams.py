# defines basic parameters needed for a rtpipe job
nsegments = 0
nthread = 16
dmarr = []
dtarr = [1]   # integer to integrate in time for independent searches
spw = []
chans = []
flagantsol = True
timesub = 'mean'
searchtype = 'image1'    # search algorithm: 'image1' is single image snr threshold
sigma_image1 = 7.0
uvres = 0   # imaging parameters. set res=size=0 to define from uv coords
npix = 0
npix_max = 1728
uvoversample = 1
savenoise = False
savecands = False
memory_limit = 20  # in GB
