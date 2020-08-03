# Load a local copy of the current ODYM branch:
import sys
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pickle
import xlrd
import pylab
from copy import deepcopy
import logging as log
import xlwt
import tqdm
import math
from scipy.stats import norm
from openpyxl import *
from scipy.optimize import curve_fit
from scipy.stats import gompertz
from sklearn.metrics import r2_score

## Define funcitons to discribe shares of different parameters for furture flows

sheet_to_df_map = pd.read_excel("/Users/fernaag/Box/BATMAN/Data/Fleet/Eric's_database/Vehicle_fleet_data.xlsx", sheet_name=None)
# Keys are: (['TOC', 'stocks', 'stocks_metadata', 'flows', 'flows_metadata', 'geography_metadata', 'process_metadata', 
# 'class_metadata', 'motor_energy_metadata', 'segment_metadata', 'unit_metadata', 'notebook_metadata', 'source', 'Potential_sources'])
vehicle_fleet = sheet_to_df_map['stocks']
# Colnames are ndex(['id', 'year_of_measurement', 'date_of_measurement', 'geo', 'process', 'vehicle_class', 'vehicle_segment', 'drive_train', 
# 'model_year','year_of_first_registraion', 'value', 'unit', 'source', 'accessed','notebook', 'footnote']

# Here we create a list of dataframes where each entry is a country. We sort on country code.
countries_fleet = []
for country in sorted(vehicle_fleet['geo'].unique()):
    countries_fleet.append(vehicle_fleet[vehicle_fleet['geo']==country])

'''
## Code for finding and deleting empty country entries. Does not usually apply
empty_countries = []
for item in range(0, len(countries_fleet)):
    if countries_fleet[item].empty == True: # This code finds and saves indexes where the list is empty
        empty_countries.append(item)
empty_countries.reverse()
for i in range(0,len(empty_countries)):
    countries_fleet.pop(empty_countries[i]) # This code deletes empty dataframes
'''
## Take only passenger fleet in consideration and seprate the dataset by source
passenger_fleet= []
for country in range(0, len(countries_fleet)):
    for source in countries_fleet[country]['source'].unique():
        c = countries_fleet[country][(countries_fleet[country]['vehicle_class']== 'EUM1') | (countries_fleet[country]['vehicle_class']=='OIPC') | \
            (countries_fleet[country]['vehicle_class']=='INCAR')| (countries_fleet[country]['vehicle_class']=='USTSU')]
        c.drop(['id','process', 'footnote', 'notebook', 'year_of_first_registraion', 'date_of_measurement'], axis=1, inplace=True) # Dropped these values either for relevance or because contained only NaN values
        passenger_fleet.append(c[c['source']==source])


fleet = []
variations = []
cleaned_data = []
segments = []
drive_trains = []
for dataset in passenger_fleet:
# To get all possible combinations of segment and drive train we can save this as a groupby object
    clean = dataset.groupby(['year_of_measurement', 'geo', 'vehicle_class', 'vehicle_segment', 'drive_train', 'model_year', 'unit', 'source', 'accessed'], as_index=False).sum().groupby(['vehicle_segment', 'drive_train'])
    cleaned_data.append(clean)
    # To get total fleet before 2012 we agggregate all sements. For validation we also aggregate the other years
    all_seg_all_dt = dataset.groupby(['year_of_measurement',  'geo', 'vehicle_class', 'drive_train', 'model_year', 'unit', 'source', 'accessed'], as_index=False).sum()
    all_seg_all_dt = all_seg_all_dt[all_seg_all_dt['drive_train']=='all'].drop_duplicates(subset=['year_of_measurement'])
    fleet.append(all_seg_all_dt)
    '''
# This code is for verifying that the sum of all segments is equal to the total fleet
    try:
        variation = 1- (all_seg_all_dt.set_index('year_of_measurement')['value'] / clean.get_group(('all', 'all')).set_index('year_of_measurement')['value']) # values are identical -> sum can be used as total fleet
        variation = pd.concat([all_seg_all_dt.set_index('year_of_measurement')['geo'], all_seg_all_dt.set_index('year_of_measurement')['source'], variation], axis=1)
        variations.append(variation)
    except:
        pass
'''
    # Since the sum of all segments is equal to the total fleet, we use the aggregation as the total and calculate the shares of each respective segment
    # There is no need to do this loop for each individual segment, as the datasets come with either all or non of them
    try:
        share_seg_a151 = clean.get_group(('a151', 'all'))['value'].reset_index(drop=True) / all_seg_all_dt['value'].reset_index(drop=True)
        share_seg_b100 = clean.get_group(('b100', 'all'))['value'].reset_index(drop=True) / all_seg_all_dt['value'].reset_index(drop=True)
        share_seg_b125 = clean.get_group(('b125', 'all'))['value'].reset_index(drop=True) / all_seg_all_dt['value'].reset_index(drop=True)
        share_seg_b150 = clean.get_group(('b150', 'all'))['value'].reset_index(drop=True) / all_seg_all_dt['value'].reset_index(drop=True)
        segment_shares = pd.concat([all_seg_all_dt['year_of_measurement'].reset_index(drop=True), all_seg_all_dt['geo'].reset_index(drop=True), all_seg_all_dt['source'].reset_index(drop=True), share_seg_b100, share_seg_b125, share_seg_b150, \
            share_seg_a151], axis=1, keys=['year', 'geo', 'source','b100', 'b125', 'b150', 'a151']).set_index('year')
        segments.append(segment_shares)
    except KeyError:
        #print('Segments not found for country {} and source {}'.format(dataset['geo'].iloc[0], dataset['source'].iloc[0]))
        pass

    # Drive train shares: PHEV and HEV only present for 2016 - 2018, BEV, ICE for 2012 - 2018. Assume BEV 0 for all times before
    # The following loops devide the fleet per drive train by the total fleet. Since the lengths differ, we make the comparisons based on the year of reporting. 
    # ICE has very low numbers until 2015, probably due to the mistake with OTH category identified earlier TODO: sum the ICE and the OTH categories before comming to this point
    try:
        ICE_share = []
        BEV_share = []
        PHEV_share = []
        HEV_share = []
        OTH_share = []
        time_point = []
        for i in range(0,len(clean.get_group(('all', 'ICE'))['value'])):
            for j in range(0,len(all_seg_all_dt['value'])):
                if clean.get_group(('all', 'ICE'))['year_of_measurement'].iloc[i]\
                    == all_seg_all_dt['year_of_measurement'].iloc[j]: # This makes sure that we are actually looking at the same year in the two datasets
                    year = all_seg_all_dt['year_of_measurement'].iloc[j]
                    geo = all_seg_all_dt['geo'].iloc[j]
                    source = all_seg_all_dt['source'].iloc[j]
                    result = float(np.array(clean.get_group(('all', 'ICE'))['value'][clean.get_group(('all', 'ICE'))['year_of_measurement']== year].reset_index(drop=True)\
                        / all_seg_all_dt['value'][all_seg_all_dt['year_of_measurement']==year].reset_index(drop=True))) # Here we create the share of the desired drive train
                    ICE_share.append([year, geo, source, result])
    except KeyError:
        #print('Drive train ICE not found for country {} and source {}'.format(dataset['geo'].iloc[0], dataset['source'].iloc[0]))
        pass
    try:
        for i in range(0,len(clean.get_group(('all', 'BEV'))['value'])):
            for j in range(0,len(all_seg_all_dt['value'])):
                if clean.get_group(('all', 'BEV'))['year_of_measurement'].iloc[i]\
                    == all_seg_all_dt['year_of_measurement'].iloc[j]:
                    year = all_seg_all_dt['year_of_measurement'].iloc[j]
                    result = float(np.array(clean.get_group(('all', 'BEV'))['value'][clean.get_group(('all', 'BEV'))['year_of_measurement']== year].reset_index(drop=True)\
                        / all_seg_all_dt['value'][all_seg_all_dt['year_of_measurement']==year].reset_index(drop=True)))
                    BEV_share.append([year, result])
    except KeyError:
        #print('Drive train BEV not found for country {} and source {}'.format(dataset['geo'].iloc[0], dataset['source'].iloc[0]))
        pass
    try:
        for i in range(0,len(clean.get_group(('all', 'HEV'))['value'])):
            for j in range(0,len(all_seg_all_dt['value'])):
                if clean.get_group(('all', 'HEV'))['year_of_measurement'].iloc[i]\
                    == all_seg_all_dt['year_of_measurement'].iloc[j]:
                    year = all_seg_all_dt['year_of_measurement'].iloc[j]
                    result = float(np.array(clean.get_group(('all', 'HEV'))['value'][clean.get_group(('all', 'HEV'))['year_of_measurement']== year].reset_index(drop=True)\
                        / all_seg_all_dt['value'][all_seg_all_dt['year_of_measurement']==year].reset_index(drop=True)))
                    HEV_share.append([year, result])
    except KeyError:
        #print('Drive train HEV not found for country {} and source {}'.format(dataset['geo'].iloc[0], dataset['source'].iloc[0]))
        pass
    try:
        for i in range(0,len(clean.get_group(('all', 'PHEV'))['value'])):
            for j in range(0,len(all_seg_all_dt['value'])):
                if clean.get_group(('all', 'PHEV'))['year_of_measurement'].iloc[i]\
                    == all_seg_all_dt['year_of_measurement'].iloc[j]:
                    year = all_seg_all_dt['year_of_measurement'].iloc[j]
                    result = float(np.array(clean.get_group(('all', 'PHEV'))['value'][clean.get_group(('all', 'PHEV'))['year_of_measurement']== year].reset_index(drop=True)\
                        / all_seg_all_dt['value'][all_seg_all_dt['year_of_measurement']==year].reset_index(drop=True)))
                    PHEV_share.append([year, result])
    except KeyError:
        #print('Drive train PHEV not found for country {} and source {}'.format(dataset['geo'].iloc[0], dataset['source'].iloc[0]))
        pass
    try:
        for i in range(0,len(clean.get_group(('all', 'OTH'))['value'])):
            for j in range(0,len(all_seg_all_dt['value'])):
                if clean.get_group(('all', 'OTH'))['year_of_measurement'].iloc[i]\
                    == all_seg_all_dt['year_of_measurement'].iloc[j]:
                    year = all_seg_all_dt['year_of_measurement'].iloc[j]
                    result = float(np.array(clean.get_group(('all', 'OTH'))['value'][clean.get_group(('all', 'OTH'))['year_of_measurement']== year].reset_index(drop=True)\
                        / all_seg_all_dt['value'][all_seg_all_dt['year_of_measurement']==year].reset_index(drop=True)))
                    OTH_share.append([year, result])
    except KeyError:
        #print('Drive train OTH not found for country {} and source {}'.format(dataset['geo'].iloc[0], dataset['source'].iloc[0]))
        pass
    try:
        ICE_share = pd.DataFrame(ICE_share).rename(columns = { 0: 'year', 1: 'geo', 2: 'source', 3: 'ICE' }).set_index('year')
        BEV_share = pd.DataFrame(BEV_share).rename(columns = { 0: 'year', 1: 'BEV' }).set_index('year')
        OTH_share = pd.DataFrame(OTH_share).rename(columns = { 0: 'year', 1: 'OTH'}).set_index('year')
        drive_train_shares = ICE_share.join([BEV_share, OTH_share])
        drive_trains.append(drive_train_shares)
    except:
        pass
    try: 
        HEV_share = pd.DataFrame(HEV_share).rename(columns = { 0: 'year', 1: 'HEV'}).set_index('year')
        drive_train_shares = ICE_share.join([BEV_share, HEV_share, OTH_share])
        drive_trains.append(drive_train_shares)
        drive_trains.pop(-2)
    except:
        pass
    try: 
        PHEV_share = pd.DataFrame(PHEV_share).rename(columns = { 0: 'year', 1: 'PHEV'}).set_index('year')
        drive_train_shares = ICE_share.join([BEV_share, OTH_share, PHEV_share])
        drive_trains.append(drive_train_shares)
        drive_trains.pop(-2)
    except:
        pass
    try: 
        drive_train_shares = ICE_share.join([BEV_share, OTH_share, PHEV_share, HEV_share])
        drive_trains.append(drive_train_shares)
        drive_trains.pop(-2)
    except:
        pass
 
print('The main output variables are fleet, variations, cleaned_data, segments, drive_trains.')

## saving the output
import pickle
for i in range(0,len(fleet)):
    fleet[i].to_pickle('/Users/fernaag/Box/BATMAN/Coding/Norwegian_Model/Pickle_files/Stocks/Stocks_{}_{}.pkl'.format(fleet[i]['geo'].iloc[0], fleet[i]['source'].iloc[0]))
for i in range(0,len(segments)):
    segments[i].to_pickle('/Users/fernaag/Box/BATMAN/Coding/Norwegian_Model/Pickle_files/Segments/Segments_{}_{}.pkl'.format(segments[i]['geo'].iloc[0], segments[i]['source'].iloc[0]))
for i in range(0,len(drive_trains)):
    drive_trains[i].to_pickle('/Users/fernaag/Box/BATMAN/Coding/Norwegian_Model/Pickle_files/Drive_trains/Drive_trains_{}_{}.pkl'.format(drive_trains[i]['geo'].iloc[0], drive_trains[i]['source'].iloc[0]))
for i in range(0,len(passenger_fleet)):
    passenger_fleet[i].to_pickle('/Users/fernaag/Box/BATMAN/Coding/Norwegian_Model/Pickle_files/Passenger_fleet/Passenger_{}_{}.pkl'.format(passenger_fleet[i]['geo'].iloc[0], passenger_fleet[i]['source'].iloc[0]))
