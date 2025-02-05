# ssh zhukov@sungeo.isr.umich.edu
# python3
# exec(open("/data/Dropbox/SUNGEO/Data/Code/geoprocess_parallel_1.py").read())
# exec(open("/home/zhukov/Dropbox/SUNGEO/Data/Code/geoprocess_parallel_1.py").read())
# nohup python3 /data/Dropbox/SUNGEO/Data/Code/geoprocess_parallel_1.py &

# Start
print("Start geoprocess_parallel_1.py")

# Clear memory
for name in dir():
    if not name.startswith('_'):
        del globals()[name]

dir()


# Import libraries
import numpy as np
import preprocessing
import pandas as pd
pd.set_option('display.max_columns', 500)
import matplotlib as mpl
# mpl.use('Agg')
import matplotlib.pyplot as plt
# import contextily as ctx
import os
from urllib.request import urlretrieve, urlopen, Request
import pandas as pd
import geopandas
import descartes
from descartes import PolygonPatch
from shapely.geometry import Point
from shapely.geometry import box
from scipy.spatial import distance_matrix
import rasterio
import rasterio.plot
from rasterio.mask import mask as rmask
import rasterstats
import fiona
from fiona.crs import from_epsg
import pycrs
# import earthpy as et
from rasterio.warp import calculate_default_transform, reproject, Resampling
from zipfile import ZipFile
import tempfile
import re
import pyreadr
import pycountry
import gc
import socket
from joblib import Parallel, delayed  
import multiprocessing
# import pysal
import mapclassify

# Garbage collection
gc.collect()

##########################################
## Working directory
##########################################

# Set wd
os.getcwd()
# os.chdir('/data/Dropbox/SUNGEO/Data/')
if socket.gethostname() == 'sungeo':
    os.chdir('/data/Dropbox/SUNGEO/Data/')

if socket.gethostname() == 'zhukov':
    os.chdir('/home/zhukov/Dropbox/SUNGEO/Data/')

if socket.gethostname() == 'ubu':
    os.chdir('/home/zhukov/Dropbox/SUNGEO/Data/')

os.listdir('Admin/GRED')
os.listdir('Elections/CLEA/Processed')


##########################################
## Define custom functions
##########################################

# Find n largest/smallest elements in a list
def n_largest(l,n): 
    s=[]
    if len(l)>0:
        for i in range(n):
            s.append(max(l)) 
            l.remove(max(l)) 
    return(s)

def n_smallest(m,n): 
    t=[]
    if len(m)>0:
        for i in range(n):
            t.append(min(m))
            m.remove(min(m))
    return(t)


##########################################
## Load CLEA data
##########################################

# Read from RData file
result = pyreadr.read_r('Elections/CLEA/clea_lc_20190617/clea_lc_20190617.rdata')
clea = result['clea_lc_20190617']

# String replacement for some country names
clea['ctr_n'] = clea['ctr_n'].replace('UK', 'United Kingdom') 
# clea['ctr_n'] = clea['ctr_n'].replace('US', 'United States') 

##########################################
## Unzip GRED to temporary folder
##########################################

# Create temporary directory
td = tempfile.TemporaryDirectory()

# Unzip GRED to temporary directory
zipobj = ZipFile('Admin/GRED/GRED_20190215.zip','r')
zipobj.extractall(td.name)

# List of files
gredz = ZipFile.namelist(zipobj)
gredz_shp = [x for x in gredz if re.search(r"shp$",x)]


##########################################
## Loop over elections
##########################################

# Select file
f0 = 16
gredz_shp[f0]

def processInput(f0):  
    ##########################################
    ## Load GRED shapefle
    ##########################################
    # Extract country names
    cname = re.split("[^a-zA-Z\d\s:]",gredz_shp[f0])[2]
    # iso3 = pycountry.countries.get(name=cname).alpha_3
    try:
        iso3 = pycountry.countries.search_fuzzy(cname)
        iso3 = [o.alpha_3 for o in iso3][0]
    except:
        strip_nonalpha = re.compile('[^a-zA-Z]')
        iso3 = strip_nonalpha.sub('', cname)
    eyear = re.split("[^a-zA-Z\d\s:]",gredz_shp[f0])[3]
    print(str(f0) + '_' + iso3 + '_' + eyear)
    # Load shapefile
    gred_i = geopandas.read_file(td.name + '/' + gredz_shp[f0])
    # Convert column names to lower case 
    gred_i.columns = map(str.lower, gred_i.columns)
    # Set CRS
    epsg_4326 = {'init': 'EPSG:4326'}
    gred_i = gred_i.to_crs(epsg_4326)
    # # Plot
    # gred_i.plot()
    # plt.show()
    ##########################################
    ## Aggregate CLEA data to constituencies
    ##########################################
    # Extract rows corresponding to GRED file
    ctr_i = set(clea['ctr_n']).intersection(gred_i['ctr_n'])
    ctr_i = list(ctr_i)
    yr_i = set([int(i) for i in clea['yr']]).intersection([int(i) for i in gred_i['yr']])
    yr_i = list(yr_i)
    if cname != 'Ireland':
        clea_i = clea[(clea['ctr_n']==ctr_i[0]) & (clea['yr']==yr_i[0])]
    if cname == 'Ireland':
        ctr_i = cname
        clea_i = clea[(clea['ctr_n']=='Ireland') & (clea['yr']==yr_i[0])]
    # Create empty matrix
    new_columns = ['election_id','iso3','ctr','ctr_n','cst','year','yrmo','to1','pev1','vot1','vv1','cv1_margin_top2','cvs1_margin_top2','cv1_margin_seat','cvs1_margin_seat','pv1_margin_top2','pvs1_margin_top2','pv1_margin_seat','pvs1_margin_seat','can1_win_c','pty1_win_c','can1_win_p','pty1_win_p','to2','pev2','vot2','vv2','cv2_margin_top2','cvs2_margin_top2','cv2_margin_seat','cvs2_margin_seat','pv2_margin_top2','pvs2_margin_top2','pv2_margin_seat','pvs2_margin_seat','can2_win_c','pty2_win_c','can2_win_p','pty2_win_p']
    df_new = pd.DataFrame(columns=new_columns)
    # Loop over constituencies
    cstz = set(clea_i['cst'])
    cstz = list(cstz)
    for c0 in range(len(cstz)):
        # print(c0, end=" ")
        # Extract subset for cst
        clea_sub = clea_i[clea['cst']==cstz[c0]]
        # Set default values
        to1 = float('nan')
        pev1 = float('nan')
        vot1 = float('nan')
        vv1 = float('nan')
        to2 = float('nan')
        pev2 = float('nan')
        vot2 = float('nan')
        vv2 = float('nan')
        cv1_margin_top2 = float('nan')
        cvs1_margin_top2 = float('nan')
        pv1_margin_top2 = float('nan')
        pvs1_margin_top2 = float('nan')
        cv1_margin_seat = float('nan')
        cvs1_margin_seat = float('nan')
        pv1_margin_seat = float('nan')
        pvs1_margin_seat = float('nan')
        can1_win_c = ""
        pty1_win_c = ""
        can1_win_p = ""
        pty1_win_p = ""
        cv2_margin_top2 = float('nan')
        cvs2_margin_top2 = float('nan')
        pv2_margin_top2 = float('nan')
        pvs2_margin_top2 = float('nan')
        cv2_margin_seat = float('nan')
        cvs2_margin_seat = float('nan')
        pv2_margin_seat = float('nan')
        pvs2_margin_seat = float('nan')
        can2_win_c = ""
        pty2_win_c = ""
        can2_win_p = ""
        pty2_win_p = ""
        # Turnout
        if list(clea_sub['to1'])[0]>0:
            to1 = list(clea_sub['to1'])[0]
        if list(clea_sub['pev1'])[0]>0:
            pev1 = list(clea_sub['pev1'])[0]
        if list(clea_sub['vot1'])[0]>0:
            vot1 = list(clea_sub['vot1'])[0]
        if list(clea_sub['vv1'])[0]>0:
            vv1 = list(clea_sub['vv1'])[0]
        if list(clea_sub['to2'])[0]>0:
            to2 = list(clea_sub['to2'])[0]
        if list(clea_sub['pev2'])[0]>0:
            pev2 = list(clea_sub['pev2'])[0]
        if list(clea_sub['vot2'])[0]>0:
            vot2 = list(clea_sub['vot2'])[0]
        if list(clea_sub['vv2'])[0]>0:
            vv2 = list(clea_sub['vv2'])[0]
        # Candidate results (round 1)
        if len(set(clea_sub['cvs1']))>1:
            # Two largest vote-getters
            diff_top2_c = n_largest(list(clea_sub['cv1']),2)
            diff_top2_c = abs(diff_top2_c[1] - diff_top2_c[0])
            cv1_margin_top2 = diff_top2_c
            diff_top2_c = n_largest(list(clea_sub['cvs1']),2)
            diff_top2_c = abs(diff_top2_c[1] - diff_top2_c[0])
            cvs1_margin_top2 = diff_top2_c
            # Largest vote-getter w/o seat
            if len(set(clea_sub['seat']))>1:
                diff_seat_1 = n_smallest(list(clea_sub[clea_sub['seat']==1]['cv1']),1)
                diff_seat_0 = n_largest(list(clea_sub[clea_sub['seat']!=1]['cv1']),1)
                cv1_margin_seat = abs(diff_seat_1[0] - diff_seat_0[0])
                diff_seat_1 = n_smallest(list(clea_sub[clea_sub['seat']==1]['cvs1']),1)
                diff_seat_0 = n_largest(list(clea_sub[clea_sub['seat']!=1]['cvs1']),1)
                cvs1_margin_seat = abs(diff_seat_1[0] - diff_seat_0[0])
            # Identify winner
            i_win_c = list(clea_sub['cvs1']).index(max(clea_sub['cvs1']))
            can1_win_c = clea_sub['can'].iloc[i_win_c]
            pty1_win_c = clea_sub['pty_n'].iloc[i_win_c]
        # Candidate results (round 2)
        if len(set(clea_sub['cvs2']))>1:
            # Two largest vote-getters
            diff_top2_c = n_largest(list(clea_sub['cv2']),2)
            diff_top2_c = abs(diff_top2_c[1] - diff_top2_c[0])
            cv2_margin_top2 = diff_top2_c
            diff_top2_c = n_largest(list(clea_sub['cvs2']),2)
            diff_top2_c = abs(diff_top2_c[1] - diff_top2_c[0])
            cvs2_margin_top2 = diff_top2_c
            # Largest vote-getter w/o seat
            if len(set(clea_sub['seat']))>1:
                diff_seat_1 = n_smallest(list(clea_sub[clea_sub['seat']==1]['cv2']),1)
                diff_seat_0 = n_largest(list(clea_sub[clea_sub['seat']!=1]['cv2']),1)
                cv2_margin_seat = abs(diff_seat_1[0] - diff_seat_0[0])
                diff_seat_1 = n_smallest(list(clea_sub[clea_sub['seat']==1]['cvs2']),1)
                diff_seat_0 = n_largest(list(clea_sub[clea_sub['seat']!=1]['cvs2']),1)
                cvs2_margin_seat = abs(diff_seat_1[0] - diff_seat_0[0])
            # Identify winner
            i_win_c = list(clea_sub['cvs2']).index(max(clea_sub['cvs2']))
            can2_win_c = clea_sub['can'].iloc[i_win_c]
            pty2_win_c = clea_sub['pty_n'].iloc[i_win_c]
        # Party results (round 1)
        if (len(set(clea_sub['pvs1']))==1) & (len(set(clea_sub['pv1']))>1):
            clea_sub.loc[:,'pvs1'] = list(clea_sub.loc[:,'pv1']/clea_sub.loc[:,'vv1'])
        if len(set(clea_sub['pvs1']))>1:
            # Two largest vote-getters
            clea_pty_sum = clea_sub['pv1'].groupby(clea_sub['pty_n']).sum()
            if len(clea_pty_sum.index)>1:
                diff_top2_p = n_largest(list(clea_pty_sum),2)
                diff_top2_p = abs(diff_top2_p[1] - diff_top2_p[0])
                pv1_margin_top2 = diff_top2_p
                clea_pty_sum = clea_sub['pvs1'].groupby(clea_sub['pty_n']).mean()
                diff_top2_p = n_largest(list(clea_pty_sum),2)
                diff_top2_p = abs(diff_top2_p[1] - diff_top2_p[0])
                pvs1_margin_top2 = diff_top2_p
                # Largest vote-getter w/o seat
                clea_pty_sum = clea_sub.groupby(clea_sub['pty_n']).mean()
                clea_pty_max = clea_sub.groupby(clea_sub['pty_n']).max()
                if len(set(clea_sub['seat']))>1:
                    diff_seat_1 = n_smallest(list(clea_pty_sum[clea_pty_max['seat']==1]['pv1']),1)
                    diff_seat_0 = n_smallest(list(clea_pty_sum[clea_pty_max['seat']!=1]['pv1']),1)
                    if (len(diff_seat_1)>0)&(len(diff_seat_0)>0):
                        pv1_margin_seat = abs(diff_seat_1[0] - diff_seat_0[0])
                    diff_seat_1 = n_smallest(list(clea_pty_sum[clea_pty_max['seat']==1]['pvs1']),1)
                    diff_seat_0 = n_smallest(list(clea_pty_sum[clea_pty_max['seat']!=1]['pvs1']),1)
                    if (len(diff_seat_1)>0)&(len(diff_seat_0)>0):
                        pvs1_margin_seat = abs(diff_seat_1[0] - diff_seat_0[0])
                # Identify winner
                i_win_p = list(clea_sub['pvs1']).index(max(clea_sub['pvs1']))
                can1_win_p = clea_sub['can'].iloc[i_win_p]
                pty1_win_p = clea_sub['pty_n'].iloc[i_win_p]
        # Party results (round 2)
        if (len(set(clea_sub['pvs2']))==1) & (len(set(clea_sub['pv2']))>1):
            clea_sub.loc[:,'pvs2'] = list(clea_sub.loc[:,'pv2']/clea_sub.loc[:,'vv2'])
        if len(set(clea_sub['pvs2']))>1:
            # Two largest vote-getters
            clea_pty_sum = clea_sub['pv2'].groupby(clea_sub['pty_n']).sum()
            if len(clea_pty_sum.index)>1:
                diff_top2_p = n_largest(list(clea_pty_sum),2)
                diff_top2_p = abs(diff_top2_p[1] - diff_top2_p[0])
                pv2_margin_top2 = diff_top2_p
                clea_pty_sum = clea_sub['pvs2'].groupby(clea_sub['pty_n']).mean()
                diff_top2_p = n_largest(list(clea_pty_sum),2)
                diff_top2_p = abs(diff_top2_p[1] - diff_top2_p[0])
                pvs2_margin_top2 = diff_top2_p
                # Largest vote-getter w/o seat
                clea_pty_sum = clea_sub.groupby(clea_sub['pty_n']).mean()
                clea_pty_max = clea_sub.groupby(clea_sub['pty_n']).max()
                if len(set(clea_sub['seat']))>1:
                    diff_seat_1 = n_smallest(list(clea_pty_sum[clea_pty_max['seat']==1]['pv2']),1)
                    diff_seat_0 = n_smallest(list(clea_pty_sum[clea_pty_max['seat']!=1]['pv2']),1)
                    if (len(diff_seat_1)>0)&(len(diff_seat_0)>0):
                        pv2_margin_seat = abs(diff_seat_1[0] - diff_seat_0[0])
                    diff_seat_1 = n_smallest(list(clea_pty_sum[clea_pty_max['seat']==1]['pvs2']),1)
                    diff_seat_0 = n_smallest(list(clea_pty_sum[clea_pty_max['seat']!=1]['pvs2']),1)
                    if (len(diff_seat_1)>0)&(len(diff_seat_0)>0):
                        pvs2_margin_seat = abs(diff_seat_1[0] - diff_seat_0[0])
                # Identify winner
                i_win_p = list(clea_sub['pvs2']).index(max(clea_sub['pvs2']))
                can2_win_p = clea_sub['can'].iloc[i_win_p]
                pty2_win_p = clea_sub['pty_n'].iloc[i_win_p]
        # Create data frame
        df = {'election_id': list(clea_sub['id'])[0],
                'iso3': iso3,
                'ctr': list(clea_sub['ctr'])[0],
                'ctr_n': list(clea_sub['ctr_n'])[0],
                'cst': cstz[c0],
                'year': eyear,
                'yrmo': list(clea_sub['yr'])[0]*100 + list(clea_sub['mn'])[0],
                'to1': to1,
                'pev1': pev1,
                'vot1': vot1,
                'vv1': vv1,
                'cv1_margin_top2': cv1_margin_top2,
                'cvs1_margin_top2': cvs1_margin_top2,
                'cv1_margin_seat': cv1_margin_seat,
                'cvs1_margin_seat': cvs1_margin_seat,
                'pv1_margin_top2': pv1_margin_top2,
                'pvs1_margin_top2': pvs1_margin_top2,
                'pv1_margin_seat': pv1_margin_seat,
                'pvs1_margin_seat': pvs1_margin_seat,
                'can1_win_c': can1_win_c,
                'pty1_win_c': pty1_win_c,
                'can1_win_p': can1_win_p,
                'pty1_win_p': pty1_win_p,
                'to2': to2,
                'pev2': pev2,
                'vot2': vot2,
                'vv2': vv2,
                'cv2_margin_top2': cv2_margin_top2,
                'cvs2_margin_top2': cvs2_margin_top2,
                'cv2_margin_seat': cv2_margin_seat,
                'cvs2_margin_seat': cvs2_margin_seat,
                'pv2_margin_top2': pv2_margin_top2,
                'pvs2_margin_top2': pvs2_margin_top2,
                'pv2_margin_seat': pv2_margin_seat,
                'pvs2_margin_seat': pvs2_margin_seat,
                'can2_win_c': can2_win_c,
                'pty2_win_c': pty2_win_c,
                'can2_win_p': can2_win_p,
                'pty2_win_p': pty2_win_p,
                }
        df = pd.DataFrame(df, index=[c0])
        df_new = df_new.append(df)
    # Merge with map
    merged = gred_i.merge(df_new, how='left', left_on="cst", right_on="cst")
    ##########################################
    ## Visually inspect data
    ##########################################
    # Plot
    # if socket.gethostname() == 'ubu':
    variablez = ['cv1_margin_top2','pv1_margin_top2','cvs1_margin_top2','pvs1_margin_top2','cvs1_margin_seat','pvs1_margin_seat','to1']
    for variable in variablez:
        fig, ax = plt.subplots(1,1)
        ax.axis('off')
        ax.set_title(iso3 + '_' + eyear, fontdict={'fontsize': '25', 'fontweight' : '3'})
        ax.annotate('variable: ' + variable,xy=(0.1, .08), xycoords='figure fraction', horizontalalignment='left', verticalalignment='top', fontsize=12, color='#555555')
        sm = plt.cm.ScalarMappable(cmap='Blues',norm=plt.Normalize(vmin=min(merged[variable]), vmax=max(merged[variable])))
        sm.set_array([]) 
        fig.colorbar(sm)
        merged.plot(column=variable, scheme='quantiles',k=5,cmap='Blues', linewidth=.8, edgecolor='black',ax=ax)
        plt.savefig('Elections/CLEA/Maps/CLEA_' + iso3 + '_' + eyear + '_' + variable + '.png')
        plt.close(fig)
    ##########################################
    ## Export to geojson
    ##########################################
    clea_out = merged
    clea_out.to_file('Elections/CLEA/Processed/CLEA_' + iso3 + '_' + eyear + '.geojson' , driver='GeoJSON')
    return



##########################################
## Parallel processing
##########################################

# # Run single-core
# for f0 in range(0,2):
#     processInput(f0) 

# Set up parallel loop
num_cores = multiprocessing.cpu_count()

# Run parallel loop
print("numCores = " + str(num_cores))
output = Parallel(n_jobs=num_cores)(delayed(processInput)(f0) for f0 in range(0,len(gredz_shp)))  
# results = pd.concat(results)
# output


# dir()

