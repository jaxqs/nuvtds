"""
    IMPORTANT TO NOTE:
        This file was taken from the monitors file 'data_models.py' written by [author].

        The following code has been heavily edited and repurposed for the development 
        of the FUVTDS monitor by Jaq Hernandez.
"""

import pandas as pd
import os

from filesystem import find_files, data_from_exposures

FILES_SOURCE = '/grp/hst/cos2/fuv_tds_2024/data'

def get_new_data(old_new):
    header_request = {
            0: ['ROOTNAME', 'SEGMENT', 'CENWAVE', 'TARGNAME', 'OPT_ELEM', 'LIFE_ADJ'],
            1: ['DATE-OBS']
            }
    table_request = {
            1: ['WAVELENGTH', 'FLUX', 'NET', 'BACKGROUND', 'DQ_WGT']
            }

    new_files_source = os.path.join(FILES_SOURCE, old_new)
    files = find_files('*/*x1d.fits', data_dir=new_files_source)

    data_results = data_from_exposures(files,
                                           header_request=header_request,
                                           table_request=table_request)

    return data_results