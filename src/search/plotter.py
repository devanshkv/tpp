#!/usr/bin/env python3

""".

This code was lifted from your (and subsequently modified) to ensure
we could include critical functions in tpp functionality.

!!! I'm implicitly assuming things like "multiprocessing" and
    "functools" and other dependencies will be handled by the YOUR
    install. It is possible that in the future, for some reason they
    will NOT be handled by the your install. First, this needs to be
    tested. Second, we should (professionally) ensure that our conda
    environment and install package explicitly includes the
    dependencies in this script.

"""


import argparse
import logging
import os
import textwrap
from functools import partial
from multiprocessing import Pool

import matplotlib
import pandas as pd
from rich.logging import RichHandler
from rich.progress import Progress

from your.utils.misc import YourArgparseFormatter
from your.utils.plotter import plot_h5

os.environ["HDF5_USE_FILE_LOCKING"] = "FALSE"
matplotlib.use("Agg")

import h5py
import numpy as np
import pylab as plt
from matplotlib import gridspec
from scipy.signal import detrend

from your.utils.math import smad_plotter


"""

TWO OPTIONS:

1. Push candidates directly from here.

2. Have plotter return a dictionary and have tpp_pipeline push the candidates. The benefit of this is that we don't have to pass outcomesID, dbon, and all that.

import database as db

"""


def plot_h5(
    h5_file,
    save=True,
    detrend_ft=True,
    publication=False,
    mad_filter=False,
    dpi=300,
    outdir=None,
):
    """
    Plot the h5 candidates

    Args:
        mad_filter (int): use MAD filter to clip data
        h5_file (str): Name of the h5 file
        save (bool): Save the file as a png
        detrend_ft (bool): detrend the frequency time plot
        publication (bool): make publication quality plot
        dpi (int): DPI of output png (default: 300)
        outdir (str): Path to the save the files into.

    Returns:
        None

    """
    with h5py.File(h5_file, "r") as f:
        dm_time = np.array(f["data_dm_time"])
        if detrend_ft:
            freq_time = detrend(np.array(f["data_freq_time"])[:, ::-1].T)
        else:
            freq_time = np.array(f["data_freq_time"])[:, ::-1].T
        dm_time[dm_time != dm_time] = 0
        freq_time[freq_time != freq_time] = 0
        freq_time -= np.median(freq_time)
        freq_time /= np.std(freq_time)
        fch1, foff, nchan, dm, cand_id, tsamp, dm_opt, snr, snr_opt, width = (
            f.attrs["fch1"],
            f.attrs["foff"],
            f.attrs["nchans"],
            f.attrs["dm"],
            f.attrs["cand_id"],
            f.attrs["tsamp"],
            f.attrs["dm_opt"],
            f.attrs["snr"],
            f.attrs["snr_opt"],
            f.attrs["width"],
        )
        tlen = freq_time.shape[1]
        if tlen != 256:
            logging.warning(
                "Lengh of time axis is not 256. This data is probably not pre-processed."
            )
        l = np.linspace(-tlen // 2, tlen // 2, tlen)
        if width > 1:
            ts = l * tsamp * width * 1000 / 2
        else:
            ts = l * tsamp * 1000

        if mad_filter:
            freq_time = smad_plotter(freq_time, float(mad_filter))

        plt.clf()

        if publication:
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(5, 7), sharex="col")

        else:
            fig = plt.figure(figsize=(15, 10))
            gs = gridspec.GridSpec(3, 2, width_ratios=[4, 1], height_ratios=[1, 1, 1])
            ax1 = plt.subplot(gs[0, 0])
            ax2 = plt.subplot(gs[1, 0])
            ax3 = plt.subplot(gs[2, 0])
            ax4 = plt.subplot(gs[:, 1])
            to_print = []
            for key in f.attrs.keys():
                if "filelist" in key or "mask" in key:
                    pass
                elif "filename" in key:
                    to_print.append(f"filename : {os.path.basename(f.attrs[key])}\n")
                    to_print.append(f"filepath : {os.path.dirname(f.attrs[key])}\n")
                else:
                    to_print.append(f"{key} : {f.attrs[key]}\n")
            str_print = "".join(to_print)
            ax4.text(0.2, 0, str_print, fontsize=14, ha="left", va="bottom", wrap=True)
            ax4.axis("off")

        ax1.plot(ts, freq_time.sum(0), "k-")
        ax1.set_ylabel("Flux (Arb. Units)")
        ax2.imshow(
            freq_time,
            aspect="auto",
            extent=[ts[0], ts[-1], fch1, fch1 + (nchan * foff)],
            interpolation="none",
        )
        ax2.set_ylabel("Frequency (MHz)")
        ax3.imshow(
            dm_time,
            aspect="auto",
            extent=[ts[0], ts[-1], 2 * dm, 0],
            interpolation="none",
        )
        ax3.set_ylabel(r"DM (pc cm$^{-3}$)")
        ax3.set_xlabel("Time (ms)")

        plt.tight_layout()
        if save:
            if outdir:
                filename = outdir + os.path.basename(h5_file)[:-3] + ".png"
            else:
                filename = h5_file[:-3] + ".png"
            plt.savefig(filename, bbox_inches="tight", dpi=dpi)
        else:
            plt.close()

    return dict_of_cands


def save_bandpass(
    your_object, bandpass, chan_nos=None, mask=None, outdir=None, outname=None
):
    """
    Plots and saves the bandpass

    Args:
        your_object: Your object
        bandpass (np.ndarray): Bandpass of the data
        chan_nos (np.ndarray): Array of channel numbers
        mask (np.ndarray): Boolean Array of channel mask
        outdir (str): Output directory to save the plot
        outname (str): Name of the bandpass file
    """

    freqs = your_object.chan_freqs
    foff = your_object.your_header.foff

    if not outdir:
        outdir = "./"

    if chan_nos is None:
        chan_nos = np.arange(0, bandpass.shape[0])

    if not outname:
        bp_plot = outdir + your_object.your_header.basename + "_bandpass.png"
    else:
        bp_plot = outname

    fig = plt.figure()
    ax11 = fig.add_subplot(111)
    if foff < 0:
        ax11.invert_xaxis()

    ax11.plot(freqs, bandpass, "k-", label="Bandpass")
    if mask is not None:
        if mask.sum():
            logging.info("Flagged %d channels", mask.sum())
            ax11.plot(freqs[mask], bandpass[mask], "r.", label="Flagged Channels")
    ax11.set_xlabel("Frequency (MHz)")
    ax11.set_ylabel("Arb. Units")
    ax11.legend()

    ax21 = ax11.twiny()
    ax21.plot(chan_nos, bandpass, alpha=0)
    ax21.set_xlabel("Channel Numbers")

    return plt.savefig(bp_plot, bbox_inches="tight", dpi=300)




"""
-------------------------------------------
"""



def mapper(save, detrend_ft, publication, mad_filter, dpi, out_dir, h5_file):
    # maps the variables so the function will be imap friendly
    plot_h5(
        h5_file=h5_file,
        save=save,
        detrend_ft=detrend_ft,
        publication=publication,
        mad_filter=mad_filter,
        dpi=dpi,
        outdir=out_dir,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="your_h5plotter.py",
        description="Plot candidate h5 files.",
        formatter_class=YourArgparseFormatter,
        epilog=textwrap.dedent(
            """\
            This script can be used to plot and save the candidate HDF5 format files generated by your_candmaker.py or by the Candidate class. 
            """
        ),
    )
    parser.add_argument("-v", "--verbose", help="Be verbose", action="store_true")
    parser.add_argument(
        "-f", "--files", help="h5 files to be plotted", nargs="+", required=False
    )
    parser.add_argument(
        "-c", "--results_csv", help="Plot positives in results.csv", required=False
    )
    parser.add_argument(
        "--publish", help="Make publication quality plots", action="store_true"
    )
    parser.add_argument(
        "--no_detrend_ft", help="Detrend the frequency-time plot", action="store_false"
    )
    parser.add_argument(
        "--no_save", help="Do not save the plot", action="store_false", default=True
    )
    parser.add_argument(
        "-o",
        "--out_dir",
        help="Directory to save pngs (default: h5 dir)",
        type=str,
        default=None,
        required=False,
    )
    parser.add_argument(
        "--dpi",
        help="DPI of resulting PNG file (default: 300)",
        type=int,
        default=300,
        required=False,
    )
    parser.add_argument(
        "-mad",
        "--mad_filter",
        help="Median Absolute Deviation spectal clipper, default 3 sigma",
        nargs="?",
        const=3.0,
        default=False,
    )
    parser.add_argument(
        "-n",
        "--nproc",
        help="Number of processors to use in parallel (default: 4)",
        type=int,
        default=4,
        required=False,
    )
    parser.add_argument(
        "--no_progress",
        help="Do not show the tqdm bar",
        action="store_true",
    )
    parser.add_argument(
        "--no_log_file", help="Do not write a log file", action="store_true"
    )

    values = parser.parse_args()
    logging_format = (
        "%(asctime)s - %(funcName)s -%(name)s - %(levelname)s - %(message)s"
    )

    if values.verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format=logging_format,
            handlers=[RichHandler(rich_tracebacks=True)],
        )

    else:
        logging.basicConfig(
            level=logging.INFO,
            format=logging_format,
            handlers=[RichHandler(rich_tracebacks=True)],
        )

    matplotlib_logger = logging.getLogger("matplotlib")
    matplotlib_logger.setLevel(logging.INFO)

    if values.files:
        h5_files = values.files
    elif values.results_csv:
        df = pd.read_csv(values.results_csv)
        h5_files = list(df["candidate"][df["label"] == 1])
    else:
        raise ValueError(f"Need either --files or --results_csv argument.")

    with Pool(processes=values.nproc) as p:
        max_ = len(h5_files)
        func = partial(
            mapper,
            values.no_save,
            values.no_detrend_ft,
            values.publish,
            values.mad_filter,
            values.dpi,
            values.out_dir,
        )

        with Progress() as progress:
            if values.no_progress:
                task = progress.add_task("[green]Writing...", total=max_, visible=False)
            else:
                task = progress.add_task("[green]Writing...", total=max_)
            for i, _ in enumerate(p.imap_unordered(func, h5_files, chunksize=2)):
                progress.update(task, advance=1)
