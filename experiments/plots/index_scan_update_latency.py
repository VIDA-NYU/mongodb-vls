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
import locale

locale.setlocale(locale.LC_ALL, 'en_US')

xlabel = "Number of Concurrent Analytics (Index Scan)"
ylabel = "95th Percentile Latency (ms)"

width = 0.35

agg = ['1','8','16', '64']

# index queries
rates = {'testdb_1m': [750000,900000,950000],
         'testdb_10m': [7500000,9000000,9500000],
         'testdb_100m': [75000000,90000000,95000000]}

max_rate = {'testdb_1m': 1.04,
            'testdb_10m': 1.04,
            'testdb_100m': 1.04}

size = {'testdb_1m': 1000000.0,
        'testdb_10m': 10000000.0,
        'testdb_100m': 100000000.0}

# databases
db_list = ['mongodb-original','mongodb-chronos']

db_dict = {'mongodb': ['mongodb-original','mongodb-chronos']}

markers_db = {'mongodb-original': ['MongoDB','#CCCCCC', None],
              'mongodb-chronos': ['MongoDB + Chronos','#CCCCCC','//']}

markers_dir = {'testdb_10m': {'mongodb-original': ['MongoDB','#CCCCCC', None],
                             'mongodb-chronos': ['MongoDB + Chronos','#CCCCCC','//']},
               'testdb_100m':{'mongodb-original': ['MongoDB','#999999', None],
                             'mongodb-chronos': ['MongoDB + Chronos','#999999','//']}}

ind = [1]
v = 1
for i in range(3):
    v = v + width*4 + width
    ind.append(v)
    
values = {'testdb_10m':  {'mongodb-original': [],
                         'mongodb-chronos': []},
          'testdb_100m': {'mongodb-original': [],
                         'mongodb-chronos': []}}

values_str = {'testdb_10m':  {'mongodb-original': [],
                             'mongodb-chronos': []},
              'testdb_100m': {'mongodb-original': [],
                             'mongodb-chronos': []}}

yerr =   {'testdb_10m':  {'mongodb-original': [],
                         'mongodb-chronos': []},
          'testdb_100m': {'mongodb-original': [],
                         'mongodb-chronos': []}}

dirs = ['testdb_10m', 'testdb_100m']

for rate in range(3):
    
    rate_perc = 0

    for db_name in db_dict:
            
        plt.figure(figsize=(8, 6), dpi=80)
        f, ax = plt.subplots()
            
        max = 1
    
        for dir in dirs:
            for database in db_list:
                values[dir][database] = []
                values_str[dir][database] = []
                yerr[dir][database] = []
            
        for dir in dirs:
            
            rate_perc = 100 - (rates[dir][rate]*100)/size[dir]
            
            for database in db_dict[db_name]:
                
                for s in agg:
                    
                    latency_values = []
                    
                    for i in range(10):
                    
                        if database == 'mongodb-original':
                        
                            if s == '0':
                                scan_file = os.path.join('..', dir, database, 'stat_updates_' + str(rates[dir][rate]) + '_' + s + '_' + str(i) + '.out')
                            else:
                                scan_file = os.path.join('..', dir, database, 'stat_index_scan_updates_' + str(rates[dir][rate]) + '_' + s + '_' + str(i) + '.out')
                                
                        else:
                            
                            if s == '0':
                                scan_file = os.path.join('..', dir, database, 'tbb_stat_updates_' + str(rates[dir][rate]) + '_' + s + '_' + str(i) + '.out')
                            else:
                                scan_file = os.path.join('..', dir, database, 'tbb_stat_index_scan_updates_' + str(rates[dir][rate]) + '_' + s + '_' + str(i) + '.out')
                        
                        latency = None
                        try:
                            f = open(scan_file, 'r')
                            line = ''
                            while True:
                                line = f.readline()
                                if '[UPDATE], 95thPercentileLatency(ms)' in line:
                                    l = line.split()
                                    latency = float(l[-1])/100.0
                                    break
                                if not line:
                                    break
                        except:
                            pass
                        
                        if not latency:
                            print "Check File: " + scan_file
                            pass
                        else:
                            latency_values.append(latency)
                        
                    mean = sum(latency_values)/float(len(latency_values))
                    std = 0
                    for latency in latency_values:
                        std += math.pow((latency-mean), 2)
                    std = math.sqrt((std/float(len(latency_values))))
                    
                    values[dir][database].append(mean)
                    values_str[dir][database].append("%.2f +- %.2f" %(mean, std))
                    yerr[dir][database].append(std)
                    
                    if (mean+std) > max:
                        max = mean+std
                
        names = []
        refs = []
        index_ = 0
        
        for dir in dirs:
            for database in db_dict[db_name]:
                r = ax.bar([k + index_*width for k in ind],
                           values[dir][database], width,
                           color=markers_dir[dir][database][1],
                           hatch=markers_dir[dir][database][2],
                           edgecolor='black',
                           yerr=yerr[dir][database],
                           ecolor='#000000',
                           error_kw={'elinewidth':2})
                index_ += 1
                
        r1 = ax.bar(-1,-1,width,color='#CCCCCC')
        r2 = ax.bar(-1,-1,width,color='#999999')
        r3 = ax.bar(-1,-1,width,color='#FFFFFF', hatch='//')
        
        ax.legend((r1, r2, r3), ('10 Million Records','100 Million Records','MongoDB-VLS'),
                  loc='upper left', shadow=True, prop={'size':15})
        
        ax.set_xticks([m + 2*width for m in ind])
        ax.set_xticklabels(agg)
        ax.tick_params(labelsize=16)
        ax.set_xlim(0.5,8.1)
        #ax.set_ylim(0,int(max*max_rate[dir]))
        ax.set_ylim(0, 240)
        ax.grid(b=True,axis='y')
        ax.set_axisbelow(True)
        ax.set_title("Index Rate: %s%%" %(str(int(rate_perc))),
                     fontproperties=font.FontProperties(size=17,weight='bold'))
    
        fig = plt.gcf()
        fig.text(0.5, 0.03, xlabel, ha='center', va='center',
                 fontproperties=font.FontProperties(size=17,weight='bold'))
        fig.text(0.05, 0.5, ylabel, ha='center', va='center', rotation='vertical',
                 fontproperties=font.FontProperties(size=17,weight='bold'))
        
        plt.savefig('latency/tbb_index_scan_updates_95_percentile_' + db_name + '_' + str(rate_perc) + '.png', bbox_inches='tight')
        plt.clf()
        
        ## Percentages
        
        text = 'Index Rate = ' + str(rate_perc) + '%\n'
                
        for dir in dirs:
            
            text += "\n"
            
            perc_values = {'1': [],
                           '8': [],
                           '16': [],
                           '64': []}
            
            for j in range(len(agg)):
                try:
                    mongo = (values[dir]['mongodb-chronos'][j]*100.00)/float(values[dir]['mongodb-original'][j]) - 100.0
                except:
                    mongo = 0
                
                perc_values[agg[j]].append('%.2f%%' %mongo)
            
            data = [[values_str[dir]['mongodb-original'][0]] + [values_str[dir]['mongodb-chronos'][0]] + perc_values['1'],
                    [values_str[dir]['mongodb-original'][1]] + [values_str[dir]['mongodb-chronos'][1]] + perc_values['8'],
                    [values_str[dir]['mongodb-original'][2]] + [values_str[dir]['mongodb-chronos'][2]] + perc_values['16'],
                    [values_str[dir]['mongodb-original'][3]] + [values_str[dir]['mongodb-chronos'][3]] + perc_values['64']]
            
            cols = ['MongoDB', 'MongoDB-VLS', 'P']
            rows = agg
            
            row_format ="{:>24}" * (len(cols) + 1)
            text += row_format.format("", *cols) + '\n'
            for col, row in zip(rows, data):
                text += row_format.format(col, *row) + '\n'
                
        f = open('latency/tbb_index_scan_updates_95_percentile_' + db_name + '_' + str(rate_perc) + '.out', 'w')
        f.write(text)
        f.close()
            
        print text + "\n"
