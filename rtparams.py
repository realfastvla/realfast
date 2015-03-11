# defines basic parameters needed for a rtpipe job
gainfile = '14A-425_14sep03.g2'
bpfile = '14A-425_14sep03.b1'
nsegments = 50
nthread = 16
timesub = 'mean'
dmarr = [0]
dtarr = [1]   # integer to integrate in time for independent searches
searchtype = 'image1'    # search algorithm: 'image1' is single image snr threshold
sigma_image1 = 7.0
flagmode='standard'
flagantsol = True
spw = [0,1]
chans = range(6,122)+range(134,250)
uvres = 0   # imaging parameters. set res=size=0 to define from uv coords
npix = 0
