
import os
import glob

if __name__ == "__main__":
    # Change these parameters
    PID = ['14854', '15384', '15535', '15773', '16324', '16830', '17249', '17251', '17328', '17326']

    for id in PID:
        # change this to the directory where the fuv tds data will be stored
        output_dir = '/grp/hst/cos2/fuv_tds_2024/data/raw/'+id+'/'

        # grabbing the relevant data from cosmo
        data_fp = '/grp/hst/cos2/cosmo/'+id+'/'

        # the asn files and rawtags files needed to be re-calibrated later
        asn_files = glob.glob(data_fp+'*asn*.fits.gz')
        rawtag_files = glob.glob(data_fp+'*rawtag_*.fits.gz')

        # Make outdir if it doesnt exist, by LP and by PID.
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # copy files over to directory
        print(f'copying over files of {id}')
        for file in asn_files:
            os.system('cp '+file+' '+output_dir+'.')
        for file in rawtag_files:
            os.system('cp '+file+' '+output_dir+'.')

        # unzip files
        print(f'unzipping files of {id}')
        asn_files = glob.glob(output_dir+'*asn*.fits.gz')
        for file in asn_files:
            os.system('gzip -d '+ file)

        rawtag_files = glob.glob(output_dir+'*rawtag_*.fits.gz')
        for file in rawtag_files:
            os.system('gzip -d '+ file)