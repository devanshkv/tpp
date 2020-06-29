#!/usr/bin/env python3

import glob
import pandas as pd
import os
import tqdm
import logging
import argparse
logger = logging.getLogger(__name__)

def gencandcsv(candsfiles, filelist, snr_th = 6, clustersize_th = 2, dm_min = 10, dm_max = 5000, label = 1,
              outname = None, outpath=None, chan_mask=None):
    try:
        assert len(candsfiles) is not 0
    except AssertionError as err:
        logger.exception("Candsfile list is empty!")
        raise err

    try:
        assert len(filelist) is not 0
    except AssertionError as err:
        logger.exception("Filelist is empty!")
        raise err
   
    filelist.sort()
    ext = filelist[0].split('.')[-1]
    if ext == "fits" or ext == "sf" or ext == "fil":
        basename = os.path.splitext(os.path.basename(filelist[0]))[0]
    else:
        raise TypeError("Can only work with list of fits file or filterbanks")
    
    if outname is None:
        outname = basename
        
    if outpath is None:
        outpath = os.getcwd()
    
    outfile = outpath+'/'+outname+'.csv'
    
    cands_out = pd.DataFrame(columns=['file', 'snr', 'stime', 'width', 'dm', 'label', 'chan_mask_path', 'num_files'])
    cands_out.to_csv(outfile, mode='w', header=True, index=False)
    
    for file in tqdm.tqdm(candsfiles):
        cands_out = pd.DataFrame(columns=['file', 'snr', 'stime', 'width', 'dm', 'label', 'chan_mask_path', 'num_files'])
        cands = pd.read_csv(file,header=None,\
                    comment='#',delim_whitespace=True,names=['snr', 'ssample', 'stime', 'width', 'dmidx', 'dm', 
                                                                'cluster_size', 'startsamp', 'endsamp'])

        cands_filtered = cands[(cands['dm'] >= dm_min) & (cands['dm'] <= dm_max) & (cands['snr'] >= snr_th) & 
                               (cands['cluster_size'] >= clustersize_th)]
    
        if len(cands_filtered) == 0:
            logger.info(f'No candidate passes the threshold criterion in {file}')
        else:
            cands_out['dm'] = cands_filtered['dm']
            cands_out['snr'] = cands_filtered['snr']
            cands_out['width'] = cands_filtered['width']
            cands_out['stime'] = cands_filtered['stime']
            cands_out['file'] = os.path.abspath(filelist[0])
            cands_out['label'] = label
            cands_out['chan_mask_path'] = chan_mask
            cands_out['num_files'] = len(filelist)
            logger.debug(f'Writing candidates in {file} to {outfile}')
            cands_out.to_csv(outfile, mode='a', header=False, index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Your heimdall candidate csv maker",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose', help='Be verbose', action='store_true')
    parser.add_argument('-o', '--fout', help='Output file directory for candidate csv file', type=str, required=False, default = None)
    parser.add_argument('-f', '--fin', help='Input files, can be *fits or *fil or *sf', nargs='+', required=True)
    parser.add_argument('-c', '--heim_cands', help='Heimdall cand files', nargs='+', required=True)
    parser.add_argument('-k', '--channel_mask_path', help='Path of channel flags mask', required=False, type=str, default=None)
    parser.add_argument('-s', '--snr_th', help='SNR Threshold', required=False, type=float, default=6)
    parser.add_argument('-dl', '--dm_min_th', help='Minimum DM allowed', required=False, type=float, default=10)
    parser.add_argument('-du', '--dm_max_th', help='Maximum DM allowed', required=False, type=float, default=5000)
    parser.add_argument('-g', '--clustersize_th', help='Minimum cluster size allowed', required=False, type=float, default=2)
    values = parser.parse_args()

    logging_format = '%(asctime)s - %(funcName)s -%(name)s - %(levelname)s - %(message)s'

    if values.verbose:
        logging.basicConfig(level=logging.DEBUG, format=logging_format)
    else:
        logging.basicConfig(level=logging.INFO, format=logging_format)
    
    if values.channel_mask_path:
        gencandcsv(values.heim_cands, values.fin, outname=values.fout, chan_mask = values.channel_mask_path, snr_th = values.snr_th, 
                clustersize_th = values.clustersize_th, dm_min = values.dm_min_th, dm_max = values.dm_max_th)
    else:
        gencandcsv(values.heim_cands, values.fin, outname=values.fout, snr_th = values.snr_th, clustersize_th = values.clustersize_th, 
                dm_min = values.dm_min_th, dm_max = values.dm_max_th)
