#!/usr/bin/env python

"""
This code will require the following to be manually changed:
    - LP: The Lifetime Position we are currently looking at
    - PID:  The program ids that correspond to said Lifetime Position
    - datadir: The directory where the files will be stored in
    - outdir: The directory where the files will be stored in
    - fits.set_val : All of the reference files we are using for the calibration given by the postgeo group

Purpose of this code:
    This code will edit the headers of the rawtag files to the necessary new reference files
given by the post-geocorr group for each LP. Then the asn files will be run to calibrate the
rawtags with these changes in place. The calibrated products will be stored in a pre-set
output directory.
    This code calibrates files in parallel, which will reduce the time typically seen in calibrating
files. This code should be run on the latest CalCOS available and over a server for higher
computation power.
"""

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
    fluxtab = {2: 'new_phot_2_20240116.fits',
               3: 'new_phot_3_20240116.fits',
               4: 'new_phot_4_20240116.fits',
               5: 'new_phot_5_20240116.fits',
               6: 'new_phot_6_20240116.fits'}
    for myfile in rawfiles:
        hdr0 = fits.getheader(myfile, 0)
        LP = hdr0['LIFE_ADJ']

        fits.setval(myfile, 'RANDSEED', value=12345, ext=0)
        fits.setval(myfile, 'FLUXTAB', value=f'/grp/hst/cos2/fuv_tds_2024/ref_files/{fluxtab[LP]}', ext=0)
        fits.setval(myfile, 'TDSTAB', value=f'/grp/hst/cos2/fuv_tds_2024/ref_files/new_tds_20240116.fits', ext=0)

    asnfiles_run = glob.glob(datadir+'*asn*')
    parallel_cal(asnfiles_run, outdir)

    #If you do not have asn files, then run the raw files
    #parallel_cal(rawfiles_run, outdir)

if __name__ == "__main__":

    PID = ['16324', '16830', '17249', '17251', '17328', '17326']

    for id in PID:

        # where the asn and rawtag files are currently stored
        datadir = '/grp/hst/cos2/fuv_tds_2024/data/raw/'+id+'/'

        #where the calibrated products will be stored in
        outdir = '/grp/hst/cos2/fuv_tds_2024/data/new_calibrated/'+id+'/'

        #Make outdir if it doens't exist.
        if not os.path.exists(outdir):
            os.makedirs(outdir)

        edit_cal_files(datadir,outdir)