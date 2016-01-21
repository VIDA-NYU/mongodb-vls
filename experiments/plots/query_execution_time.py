# Copyright (c) 2016, New York University
#                                                                                                                                                                                 
# Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
# may not use this file except in compliance with the License. You                                                                                                                
# may obtain a copy of the License at                                                                                                                                             
#                                                                                                                                                                                 
# http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
#                                                                                                                                                                                 
# Unless required by applicable law or agreed to in writing, software                                                                                                             
# distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
# implied. See the License for the specific language governing                                                                                                                    
# permissions and limitations under the License. See accompanying                                                                                                                 
# LICENSE file.

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.font_manager as font
import math
import os
import sys

xlabel = "Number of Records"
ylabel = "Duration of an Analytic Query (s)"

width = 0.35

ind = [1]
v = 1
for i in range(1):
    v = v + width*8 + width
    ind.append(v)

#dirs = ['testdb_1m','testdb_10m','testdb_100m']
#agg = ["1,000,000", "10,000,000", "100,000,000"]
dirs = ['testdb_10m','testdb_100m']
agg = ["10,000,000", "100,000,000"]

# index queries
rates = {'testdb_1m': [750000,900000,950000],
         'testdb_10m': [7500000,9000000,9500000],
         'testdb_100m': [75000000,90000000,95000000]}

update_rates = {'testdb_1m': 10000,
                'testdb_10m': 10000,
                'testdb_100m': 10000}

size = {'testdb_1m': 1000000.0,
        'testdb_10m': 10000000.0,
        'testdb_100m': 100000000.0}

markers_list = ['mongodb-original-5','mongodb-chronos-5',
                'mongodb-original-10','mongodb-chronos-10',
                'mongodb-original-25','mongodb-chronos-25',
                'mongodb-original-100','mongodb-chronos-100']

markers_list_legend = ['mongodb-original-5',
                       'mongodb-original-10',
                       'mongodb-original-25',
                       'mongodb-original-100']

markers = {'mongodb-original-50': ['MongoDB','#EEEEEE', None, 'Index Scan - 50%'],
           'mongodb-chronos-50': ['MongoDB-VLS','#EEEEEE','//'],
           'mongodb-original-25': ['MongoDB','#DDDDDD', None, 'Index Scan - 25%'],
           'mongodb-chronos-25': ['MongoDB-VLS','#DDDDDD','//'],
           'mongodb-original-10': ['MongoDB','#BBBBBB', None, 'Index Scan - 10%'],
           'mongodb-chronos-10': ['MongoDB-VLS','#BBBBBB','//'],
           'mongodb-original-5': ['MongoDB','#999999', None, 'Index Scan - 5%'],
           'mongodb-chronos-5': ['MongoDB-VLS','#999999','//'],
           'mongodb-original-1': ['MongoDB','#777777', None, 'Index Scan - 1%'],
           'mongodb-chronos-1': ['MongoDB-VLS','#777777','//'],
           'mongodb-original-100': ['MongoDB','#FFFFFF', None, 'Full Scan'],
           'mongodb-chronos-100': ['MongoDB-VLS','#FFFFFF','//'],}

############# NO UPDATES ###################

values = {'testdb_10m': [],
          'testdb_100m': []}

mean_values = {'testdb_10m': [],
               'testdb_100m': []}

#plt.figure(figsize=(8, 6), dpi=80)
    
fig, ax = plt.subplots()
index = 0

# legend

names = []
refs = []

for marker in markers_list_legend:
    refs.append(ax.bar(-1,-1,width,color=markers[marker][1]))
    names.append(markers[marker][3])
refs.append(ax.bar(-1,-1,width,color='#FFFFFF', hatch='//'))
names.append('MongoDB-VLS  ')

for dir in dirs:
    values[dir] = []
    mean_values[dir] = []

for marker in markers_list:
    
    print "DB:", marker
    
    y = []
    yerr = [[],[]]
    
    db = marker[:marker.find('-',8)]
    rate_perc = float(marker[marker.find('-',8)+1:])
    
    for dir in dirs:
        
        print "Dir:", dir
        
        rate = int(size[dir] - (rate_perc/100)*size[dir])
        
        scan_duration = []
        
        for i in range(10):
            if db == 'mongodb-original':
                if rate == 0:
                    scan_file = os.path.join('..', dir, db, 'stat_single_scan_' + str(i) + '.out')
                else:
                    scan_file = os.path.join('..', dir, db, 'stat_single_index_scan_' + str(rate) + '_' + str(i) + '.out')
            else:
                if rate == 0:
                    scan_file = os.path.join('..', dir, db, 'tbb_stat_single_scan_' + str(i) + '.out')
                else:
                    scan_file = os.path.join('..', dir, db, 'tbb_stat_single_index_scan_' + str(rate) + '_' + str(i) + '.out')
        
            f = open(scan_file, 'r')
            line = ''
            while True:
                line = f.readline()
                if '[INDEX], AverageLatency(us)' in line:
                    l = line.split()
                    scan_duration.append(float(l[-1])/1000000.0) ## average latency
                    break
                if '[AGGREGATE], AverageLatency(us)' in line:
                    l = line.split()
                    scan_duration.append(float(l[-1])/1000000.0) ## average latency
                    break
                if not line:
                    break
                
        mean = sum(scan_duration)/float(len(scan_duration))
        log_mean = math.log10(mean*10)
        
        std = 0
        for duration in scan_duration:
            std += math.pow((duration-mean), 2)
        std = math.sqrt((std/float(len(scan_duration))))
        sem = std/float(math.sqrt(len(scan_duration)))
        
        y.append(log_mean)
        yerr[0].append(log_mean - math.log10((mean - std)*10))
        yerr[1].append(math.log10((mean + std)*10) - log_mean)
        values[dir].append("%.2f +- %.2f" %(mean, std))
        mean_values[dir].append(mean)
        
        #print "%s (%s): %.2f +- %.2f\n" %(marker, dir, mean, std)
    
    r = ax.bar([i + index*width for i in ind], y, width, color=markers[marker][1],
               yerr=yerr, ecolor='#000000', error_kw={'elinewidth':2},
               hatch=markers[marker][2], edgecolor='black')
    index += 1
    
ax.set_xlabel(xlabel, fontproperties=font.FontProperties(size=17,weight='bold'))
ax.set_ylabel(ylabel, fontproperties=font.FontProperties(size=17,weight='bold'))

ax.set_xticks([i + 4*width for i in ind])
ax.set_xticklabels(agg)
ax.set_yticks([0,1,2,3])
ax.set_yticklabels(['0.1','1','10','100'])
ax.tick_params(labelsize=16)
ax.grid(b=True,axis='y')
ax.set_axisbelow(True)
#ax.set_title("No Updates", fontproperties=font.FontProperties(size=14,weight='bold'))

ax.legend(tuple(refs), tuple(names),
          loc='upper left', shadow=True, prop={'size':15})

plt.xlim(0.5,7.5)
plt.ylim(0,4)

plt.savefig('scan_duration/scan_duration.png', bbox_inches='tight')
plt.clf()

text = ''

for i in range(0,len(markers_list_legend)*2,2):
    values['testdb_10m'].append( (mean_values['testdb_10m'][i+1]*100.00)/float(mean_values['testdb_10m'][i]) - 100.0 )
    values['testdb_100m'].append( (mean_values['testdb_100m'][i+1]*100.00)/float(mean_values['testdb_100m'][i]) - 100.0 )

data = [values['testdb_10m'],
        values['testdb_100m']]

cols = ['MongoDB (5%)', 'MongoDB-VLS (5%)',
        'MongoDB (10%)', 'MongoDB-VLS (10%)',
        'MongoDB (25%)', 'MongoDB-VLS (25%)',
        'MongoDB (100%)', 'MongoDB-VLS (100%)',
        'P (5%)', 'P (10%)', 'P (25%)', 'P (100%)']
rows = agg

row_format ="{:>24}" * (len(cols) + 1)
text += row_format.format("", *cols) + '\n'
for col, row in zip(rows, data):
    text += row_format.format(col, *row) + '\n'
text += '\n\n'

print "No Updates"
print text
f = open('scan_duration/scan_duration.out', 'w')
f.write(text)
f.close()

############# WITH UPDATES ###################

values = {'testdb_10m': [],
          'testdb_100m': []}

mean_values = {'testdb_10m': [],
               'testdb_100m': []}

#plt.figure(figsize=(8, 6), dpi=80)
    
fig, ax = plt.subplots()
index = 0

# legend

names = []
refs = []

for marker in markers_list_legend:
    refs.append(ax.bar(-1,-1,width,color=markers[marker][1]))
    names.append(markers[marker][3])
refs.append(ax.bar(-1,-1,width,color='#FFFFFF', hatch='//'))
names.append('MongoDB-VLS  ')

for dir in dirs:
    values[dir] = []
    mean_values[dir] = []

for marker in markers_list:
    
    print "DB:", marker
    
    y = []
    yerr = [[],[]]
    
    db = marker[:marker.find('-',8)]
    rate_perc = float(marker[marker.find('-',8)+1:])
    
    for dir in dirs:
        
        print "Dir:", dir
        
        rate = int(size[dir] - (rate_perc/100)*size[dir])
        
        scan_duration = []
        
        for i in range(10):
            if db == 'mongodb-original':
                if rate == 0:
                    scan_file = os.path.join('..', dir, db, 'stat_scan_updates_' + str(update_rates[dir]) + '_1_' + str(i) + '.out')
                else:
                    scan_file = os.path.join('..', dir, db, 'stat_index_scan_updates_' + str(rate) + '_1_' + str(i) + '.out')
            else:
                if rate == 0:
                    scan_file = os.path.join('..', dir, db, 'tbb_stat_scan_updates_' + str(update_rates[dir]) + '_1_' + str(i) + '.out')
                else:
                    scan_file = os.path.join('..', dir, db, 'tbb_stat_index_scan_updates_' + str(rate) + '_1_' + str(i) + '.out')
        
            f = open(scan_file, 'r')
            line = ''
            while True:
                line = f.readline()
                if '[INDEX], AverageLatency(us)' in line:
                    l = line.split()
                    scan_duration.append(float(l[-1])/1000000.0) ## average latency
                    break
                if '[AGGREGATE], AverageLatency(us)' in line:
                    l = line.split()
                    scan_duration.append(float(l[-1])/1000000.0) ## average latency
                    break
                if not line:
                    break
                
        mean = sum(scan_duration)/float(len(scan_duration))
        log_mean = math.log10(mean*10)
        
        std = 0
        for duration in scan_duration:
            std += math.pow((duration-mean), 2)
        std = math.sqrt((std/float(len(scan_duration))))
        sem = std/float(math.sqrt(len(scan_duration)))
        
        y.append(log_mean)
        yerr[0].append(log_mean - math.log10((mean - std)*10))
        yerr[1].append(math.log10((mean + std)*10) - log_mean)
        values[dir].append("%.2f +- %.2f" %(mean, std))
        mean_values[dir].append(mean)
        
        #print "%s (%s): %.2f +- %.2f\n" %(marker, dir, mean, std)
    
    r = ax.bar([i + index*width for i in ind], y, width, color=markers[marker][1],
               yerr=yerr, ecolor='#000000', error_kw={'elinewidth':2},
               hatch=markers[marker][2], edgecolor='black')
    index += 1
    
ax.set_xlabel(xlabel, fontproperties=font.FontProperties(size=17,weight='bold'))
#ax.set_ylabel(ylabel, fontproperties=font.FontProperties(size=17,weight='bold'))

ax.set_xticks([i + 4*width for i in ind])
ax.set_xticklabels(agg)
ax.set_yticks([0,1,2,3,4])
#ax.set_yticklabels(['0.1','1','10','100','1,000'])
ax.set_yticklabels([])
ax.tick_params(labelsize=16)
ax.grid(b=True,axis='y')
ax.set_axisbelow(True)
#ax.set_title("With Concurrent Updates", fontproperties=font.FontProperties(size=14,weight='bold'))

ax.legend(tuple(refs), tuple(names),
          loc='upper left', shadow=True, prop={'size':15})

plt.xlim(0.5,7.5)
plt.ylim(0,4.8)

plt.savefig('scan_duration/scan_duration_updates.png', bbox_inches='tight')
plt.clf()

text = ''

for i in range(0,len(markers_list_legend)*2,2):
    values['testdb_10m'].append( (mean_values['testdb_10m'][i+1]*100.00)/float(mean_values['testdb_10m'][i]) - 100.0 )
    values['testdb_100m'].append( (mean_values['testdb_100m'][i+1]*100.00)/float(mean_values['testdb_100m'][i]) - 100.0 )

data = [values['testdb_10m'],
        values['testdb_100m']]

cols = ['MongoDB (5%)', 'MongoDB-VLS (5%)',
        'MongoDB (10%)', 'MongoDB-VLS (10%)',
        'MongoDB (25%)', 'MongoDB-VLS (25%)',
        'MongoDB (100%)', 'MongoDB-VLS (100%)',
        'P (5%)', 'P (10%)', 'P (25%)', 'P (100%)']
rows = agg

row_format ="{:>24}" * (len(cols) + 1)
text += row_format.format("", *cols) + '\n'
for col, row in zip(rows, data):
    text += row_format.format(col, *row) + '\n'
text += '\n\n'

print "With Concurrent Updates"
print text
f = open('scan_duration/scan_duration_updates.out', 'w')
f.write(text)
f.close()
