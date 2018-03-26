"""A module to help visualize Mongo Databases
"""

import numpy as np
import pandas as pd

# Matplotlib
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.path import Path
import matplotlib.patches as patches

def Ternary2D(db, cvals=None, show=False):
    # initialize plot
    fig, ax = plt.subplots(figsize=(6,3.6))
    plt.xlim([-0.75, 1.25])
    plt.ylim([-0.18, 1.02])
    plt.xticks([])
    plt.yticks([])

    # form and color ternary triangles
    codes = [Path.MOVETO, Path.LINETO, Path.LINETO, Path.CLOSEPOLY]
    pathPos = Path([(0., 0.), (1., 0.), (0.5, 0.866), (0., 0.)], codes)
    pathNeg = Path([(0., 0.), (-0.5, 0.866), (0.5, 0.866), (0., 0.)], codes)
    ax.add_patch(patches.PathPatch(pathPos, facecolor='white', lw=2))
    ax.add_patch(patches.PathPatch(pathNeg, facecolor='#fff5ee', lw=2))

    # form and color HB/MX/DD dividing lines
    ax.plot([0.667, 0.5], [0., 0.866], color='#eeb4b4', lw=1)
    ax.plot([-0.333, 0.5], [0.577, 0.866], color='#eeb4b4', lw=1)
    ax.plot([0.333, 0.5], [0., 0.866], color='#7ec0ee', lw=1)
    ax.plot([-0.167, 0.5], [0.289, 0.866], color='#7ec0ee', lw=1)

    # label corners
    ax.text(1.0, -0.15, u'Elst (\u2212)',
        verticalalignment='bottom', horizontalalignment='center',
        family='Times New Roman', weight='bold', fontsize=18)
    ax.text(0.5, 0.9, u'Ind (\u2212)',
        verticalalignment='bottom', horizontalalignment='center',
        family='Times New Roman', weight='bold', fontsize=18)
    ax.text(0.0, -0.15, u'Disp (\u2212)',
        verticalalignment='bottom', horizontalalignment='center',
        family='Times New Roman', weight='bold', fontsize=18)
    ax.text(-0.5, 0.9, u'Elst (+)',
        verticalalignment='bottom', horizontalalignment='center',
        family='Times New Roman', weight='bold', fontsize=18)

    elst = db['SAPT ELST ENERGY']
    ind = db['SAPT IND ENERGY']
    disp = db['SAPT DISP ENERGY']

    Ftop = ind.abs() / (elst.abs() + ind.abs() + disp.abs())
    Fright = elst.abs() / (elst.abs() + ind.abs() + disp.abs())

    xvals = 0.5 * Ftop + Fright
    yvals = 0.866 * Ftop

    mask = elst > 0
    xvals[mask] = 0.5 * (Ftop[mask] - Fright[mask])
    yvals[mask] = 0.866 * (Ftop[mask] + Fright[mask])

    if cvals is None:
        cvals = 0.5 + (xvals - 0.5)/(1 - Ftop)
    sc = ax.scatter(xvals, yvals, c=cvals, s=15, marker="o", \
        cmap=mpl.cm.jet, edgecolor='none', vmin=0, vmax=1, zorder=10)

    if show:
        plt.show()

    return fig, ax

