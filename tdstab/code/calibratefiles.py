#!/usr/bin/env python

import calcos
from astropy.io import fits
import glob
import os
from functools import partial
import multiprocessing as mp


# sets up the parallelization
def parallel_cal(files, outputfolder):
    pool = mp.Pool(processes=15)
    calfunc = partial(calibrate_files, outputfolder)
    pool.map(calfunc, files)

# defines calcos
def calibrate_files(outputfolder, item):
    print(item, outputfolder)
    calcos.calcos(item, outdir=outputfolder)

# edit the headers
def edit_cal_files(datadir, outdir):

    #First we want to get all rawtag files and set the proper headers:
    rawfiles=glob.glob(datadir+'*rawtag*')
    for myfile in rawfiles:
        hdr0 = fits.getheader(myfile, 0)

        fits.setval(myfile, 'RANDSEED', value=12345, ext=0)
        fits.setval(myfile, 'TDSTAB', value=f'/grp/hst/cos2/fuv_tds_2024/ref_files/new_tds_20240116.fits', ext=0)

    asnfiles_run = glob.glob(datadir+'*asn*')
    parallel_cal(asnfiles_run, outdir)

    #If you do not have asn files, then run the raw files
    #parallel_cal(rawfiles_run, outdir)

if __name__ == "__main__":

    PID = ''

    # where the asn and rawtag files are currently stored
    datadir = '/grp/hst/cos2/fuv_tds_2024/data/raw/'+PID+'/'

    #where the calibrated products will be stored in
    outdir = '/grp/hst/cos2/fuv_tds_2024/data/new_calibrated/'+PID+'/'

    #Make outdir if it doens't exist.
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    edit_cal_files(datadir,outdir)