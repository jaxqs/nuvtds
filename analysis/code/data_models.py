"""
    IMPORTANT TO NOTE:
        This file was taken from the monitors file 'data_models.py' written by [author].
"""
import os
import pandas as pd

from filesystem import find_files, data_from_exposures

FILES_SOURCE = '/grp/hst/cos2/cosmo'

def get_program_ids(pid_file):
    """Retrieve the program IDs from the given text file."""
    programs_df = pd.read_csv(pid_file, delim_whitespace=True)
    all_programs = []
    for col, col_data in programs_df.items():
        all_programs += col_data.to_numpy(dtype=str).tolist()

    return all_programs


def get_new_data(PROGRAMS):
    header_request = {
            0: ['ROOTNAME', 'CENWAVE', 'TARGNAME', 'OPT_ELEM'],
            1: ['EXPSTART', 'DATE-OBS']
            }
    table_request = {
            1: ['WAVELENGTH', 'FLUX', 'NET', 'DQ_WGT']
            }

    files = []

    program_ids = get_program_ids(PROGRAMS)

    for prog_id in program_ids:
        new_files_source = os.path.join(FILES_SOURCE, prog_id)
        files += find_files('*x1d.fits*', data_dir=new_files_source)

    data_results = data_from_exposures(files,
                                           header_request=header_request,
                                           table_request=table_request)
    
    data_results = pd.DataFrame(data_results)

    return data_results