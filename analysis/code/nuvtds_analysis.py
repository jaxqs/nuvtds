from scipy.optimize import curve_fit
from scipy.stats import binned_statistic
import matplotlib.pyplot as plt
import numpy as np 
import synphot as syn
import os
from data_models import get_new_data
import pandas as pd

def binned(cenwave, stripe, x, y, wgt, binsize): 
    cenwaves = {
        # G185M
        '1786': {'NUVA': [1670, 1705], 'NUVB': [1769, 1804], 'NUVC': [1868, 1903]},
        '1921': {'NUVA': [1804, 1839], 'NUVB': [1903, 1938], 'NUVC': [2002, 2037]},
        '2010': {'NUVA': [1894, 1929], 'NUVB': [1993, 2028], 'NUVC': [2092, 2127]},

        # DUMMY
        #'2950': {'NUVA': [1670, 1705], 'NUVB': [1769, 1804], 'NUVC': [1868, 1903]}, #G230L
        '2739': {'NUVA': [1804, 1839], 'NUVB': [1903, 1938], 'NUVC': [2002, 2037]}, #G285M
        '3094': {'NUVA': [1894, 1929], 'NUVB': [1993, 2028], 'NUVC': [2092, 2127]}, #G285M
        '2617': {'NUVA': [1894, 1929], 'NUVB': [1993, 2028], 'NUVC': [2092, 2127]}, #G285M
        #'3360': {'NUVA': [1894, 1929], 'NUVB': [1993, 2028], 'NUVC': [2092, 2127]}, #G230L

        # G225M
        '2186': {'NUVA': [2070, 2105], 'NUVB': [2169, 2204], 'NUVC': [2268, 2303]},
        '2217': {'NUVA': [2101, 2136], 'NUVB': [2200, 2235], 'NUVC': [2299, 2334]},
        '2233': {'NUVA': [2117, 2152], 'NUVB': [2215, 2250], 'NUVC': [2314, 2349]},
        '2250': {'NUVA': [2134, 2169], 'NUVB': [2233, 2268], 'NUVC': [2332, 2367]},
        '2268': {'NUVA': [2152, 2187], 'NUVB': [2251, 2286], 'NUVC': [2350, 2385]},
        '2283': {'NUVA': [2167, 2202], 'NUVB': [2266, 2301], 'NUVC': [2364, 2399]},
        '2306': {'NUVA': [2190, 2225], 'NUVB': [2288, 2323], 'NUVC': [2387, 2422]},
        '2410': {'NUVA': [2294, 2329], 'NUVB': [2393, 2428], 'NUVC': [2492, 2527]},

        # G230L
        '2635': {'NUVA': [1334, 1733], 'NUVB': [2435, 2834], 'NUVC': [1768, 1967]},
        '2410': {'NUVA': [1650, 2050], 'NUVB': [2750, 3150], 'NUVC': [1900, 2100]}
        }
    
    if (str(cenwave) == '2635') | (str(cenwave) == '2410') & (binsize == 35):
        binsize = (cenwaves[str(cenwave)][stripe][1] - cenwaves[str(cenwave)][stripe][0])

    # sum
    net, edges, _ = binned_statistic(x, y*wgt, statistic = 'sum', 
                                   bins = (cenwaves[str(cenwave)][stripe][1] - 
                                           cenwaves[str(cenwave)][stripe][0]) / 
                                           binsize)
    dqwgt, edges, _ = binned_statistic(x, wgt, statistic = 'sum', 
                                   bins = (cenwaves[str(cenwave)][stripe][1] - 
                                           cenwaves[str(cenwave)][stripe][0]) / 
                                           binsize)
    
    s = net / dqwgt

    edges = edges[:-1]+np.diff(edges)/2
    
    return(s, edges)

def relative_net(data):
    NUV = ['NUVA', 'NUVB', 'NUVC']

    rel_net_df = pd.DataFrame(columns=['NET', 'WAVELENGTH', 'DQ_WGT'])

    for grating, cenwave in zip(data['OPT_ELEM'], data['CENWAVE']):
        min_mjd = min(data['EXPSTART'].loc[(data['OPT_ELEM'] == grating) & (data['CENWAVE'] == cenwave)])
        min_net = data['NET'].loc[(data['EXPSTART'] == min_mjd) &
                                  (data['OPT_ELEM'] == grating) & 
                                  (data['CENWAVE'] == cenwave)]
        min_wl = data['WAVELENGTH'].loc[(data['EXPSTART'] == min_mjd) &
                                  (data['OPT_ELEM'] == grating) & 
                                  (data['CENWAVE'] == cenwave)]
        min_dqwgt = data['DQ_WGT'].loc[(data['EXPSTART'] == min_mjd) &
                                  (data['OPT_ELEM'] == grating) & 
                                  (data['CENWAVE'] == cenwave)]
        
        net = data['NET'].loc[(data['OPT_ELEM'] == grating) & (data['CENWAVE'] == cenwave)]
        wl = data['WAVELENGTH'].loc[(data['OPT_ELEM'] == grating) & (data['CENWAVE'] == cenwave)]
        dq_wgt = data['DQ_WGT'].loc[(data['OPT_ELEM'] == grating) & (data['CENWAVE'] == cenwave)]

        nets = []
        wls = []
        dq_wgts = []
        for i, stripe in enumerate(NUV):
            min_net, min_wl = binned(
                cenwave, 
                stripe, 
                min_wl[i],
                min_net[i],
                min_dqwgt[i],
                35)
            net, wl = binned(
                cenwave, 
                stripe, 
                wl[i],
                net[i],
                dq_wgt[i],
                35)
            
            net = net / min_net
            nets.append(net)
            wls.append(wl)
            dq_wgts.append(wl)
        rel_net_df['NET'] = nets
        rel_net_df['WAVELENGTH'] = wls
        
            

 #   for grating, cenwave in zip(data['OPT_ELEM'], data['CENWAVE']):
 #       for net in data['NET'].loc[(data['OPT_ELEM'] == grating) & (data['CENWAVE'] == cenwave)]:
 #           for i, stripe in enumerate(NUV):
 #               net[i] = net[i]/min(net[i])

if __name__ == "__main__":

    # change this as needed
    data_list = '/user/jhernandez/nuvtds/analysis/code/nuvtds_analysis_list.dat'

    data = get_new_data(data_list)

    relative_net(data)

    #for flux in data['FLUX'].loc[(data['OPT_ELEM'] == 'G225M') & (data['CENWAVE'] == 2186)]:
    #    print(len(flux))