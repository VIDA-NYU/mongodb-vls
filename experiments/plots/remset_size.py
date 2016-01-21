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
import locale
import math
import os
import sys

locale.setlocale(locale.LC_ALL, 'en_US')

xlabel = "Time (s)"
ylabel = "Remset Size (million records)"

dirs = ['testdb_100m']
#dirs = ['testdb_1m','testdb_10m']
#dirs = ['testdb_100k']
#dirs = ['testdb_1m']

#aggs = ['8','16','64']
aggs = ['8','16']

#db_list = ['chronos_mongodb','chronos_tokumx']
db_list = ['mongodb-chronos']

# update rates
rates = {#'testdb_100k': [1000],
         'testdb_1m': [10000],
         #'testdb_1m': [10000],
         'testdb_10m': [10000],
         'testdb_100m': [10000,100000]}
         #'testdb_100m': [10000]}

str_rates = {'testdb_100m': ["10,000","100,000"]}

size_db = {'testdb_100k': 100000.0,
        'testdb_1m':   1000000.0,
        'testdb_10m':  10000000.0,
        'testdb_100m': 100000000.0}

db = {'mongodb-chronos': ['MongoDB + Chronos','#000000','-','--',':']}

for dir in dirs:

    for i in range(len(rates[dir])):
        
        for agg in aggs:
        
            plt.figure(figsize=(8, 6), dpi=80)
        
            f, ax = plt.subplots()
                
            increment = 0
            
            max_sizes = []
            max_times = []
            
            text = ""
            
            for database in db_list:
                first = 0
                size = []
                size_worst = []
                size_reclamation = []
                time = []
                max_size = 0
                max_size_w = 0
                max_size_r = 0
                max_time = 0
                max_time_w = 0
                max_id = 0
                scan_file = os.path.join('..', dir, database, 'stat_scan_update_queue_size_' + str(rates[dir][i]) + '_' + agg + '.out')
                
                t_i = 0
                t_f = 0
    
                t_ = []
    
                # With reclamation
    
                x1_i = 0
                x1_f = 0
    
                x1 = []
    
                # Without reclamation
    
                x2_i = 0
                x2_f = 0
    
                x2 = []
    
                begin_fill = False
                try:
                    f = open(scan_file, 'r')
                    line = ''
                    id_ = -1
                    while True:
                        line = f.readline()
                        if 'Queue Size:' in line:
                            l = line.split("/")
                            s = math.log10((float(l[0].split()[-1])/1000000.0 + 0.1)*10)
                            s_w = math.log10((float(l[1].split()[-1])/1000000.0 + 0.1)*10)
                            s_r = math.log10((float(l[2].split()[-1])/1000000.0 + 0.1)*10)
                            t = float(l[3].split()[-1])
                            if first == 0:
                                first = t - 1
                            size.append(s)
                            size_worst.append(s_w)
                            size_reclamation.append(s_r)
                            #time.append((t - first)/1000000000.0)
                            time.append(t - first)
                            id_ += 1
                            
                            if s > max_size:
                                max_size = s
                                max_time = t - first
                            if s_w > max_size_w:
                                max_size_w = s_w
                                max_time = t - first
                                max_time_w = t = first
                                id = id_
                            if s_r > max_size_r:
                                max_size_r = s_r
                                max_time = t - first
    
                            if s_r > s:
                                begin_fill = True
                                t_i = t - first
                             
                            if begin_fill:
                                x1.append(s)
                                x2.append(s_r)
                                t_.append(t - first)
                                                        
                        if not line:
                            t_f = t - first
                            print "t_f:", t_f
                            x1_f = s
                            print "x1_f:", x1_f
                            x2_f = s_r
                            print "x2_f:", x2_f
                            break
                        if '[OVERALL], RunTime' in line:
                            t_f = t - first
                            print "t_f:", t_f
                            x1_f = s
                            print "x1_f:", x1_f
                            x2_f = s_r
                            print "x2_f:", x2_f
                            #print db[database][0] + ": " + line.split()[-1]
                            break
                except Exception as e:
                    print str(e)
                    pass
                
                max_sizes.append(max_size_w)
                max_times.append(max_time)
                
                #time.append(time[-1] + 0.00001)
                time.append(time[-1] + 10000)
                size.append(0)
                size_worst.append(0)
                size_reclamation.append(0)
    
                print "Memory Reclamation:", time[-1] - max_time
                print "Rate: %s%%" %str(((time[-1]-max_time)/time[-1])*100)
    
                ax.fill_between(t_, x2, x1, facecolor='#777777', edgecolor='none', interpolate=False)
                
                #print "Size (time):", len(time)
                #print "Size (size):", len(size) 

                # arrow

                increase = ""
                if (rates[dir][i] == 10000):
                    if (agg == "8"):
                        increase = "7.9x"
                    elif (agg == "64"):
                        increase = "60x"
                    else:
                        increase = "15.9x"
                else:
                    if (agg == "8"):
                        increase = "7.8x"
                    elif (agg == "64"):
                        increase = "62x"
                    else:
                        increase = "15.9x"

                ax.annotate("", xy=(time[id], size_worst[id]), xycoords='data', xytext=(time[id], size[id]), textcoords='data',
                         arrowprops=dict(arrowstyle="<->", connectionstyle="arc3", color="#666666", lw=3))

                ax.text(time[id], size[id] + (size_worst[id] - size[id])/3, increase, ha='center', va='center',
                        fontproperties=font.FontProperties(size=15,weight='bold'),
                        bbox=dict(boxstyle="round", fc="w", ec="w"))   

                # plots

                ax.plot(time, size, color=db[database][1], linewidth=1.3,
                        linestyle=db[database][2], label="Shared Remset with Immediate Reclamation")
                ax.plot(time, size_reclamation, color=db[database][1], linewidth=1.4,
                        linestyle=db[database][4], label="Shared Remset without Immediate Reclamation")
                ax.plot(time, size_worst, color=db[database][1], linewidth=1.3,
                        linestyle=db[database][3], label="%s Remsets"%agg)
                
                ax.legend(loc='upper left', shadow=True, prop={'size':14})
                
    #            ax.text(0.04, 0.9 - increment, "%s - Max Queue Size = %s"
    #                         %(db[database][0], locale.format("%d", max_size*10000.0, grouping=True)),
    #                         size=8.5, color=db[database][1],
    #                         transform=ax.transAxes)
    
                text += "%s - Max Remset Size = %s\n"%(db[database][0], locale.format("%d", max_size*1000000.0, grouping=True))
                text += "%s - Worst Case Max Remset Size = %s\n"%(db[database][0], locale.format("%d", max_size_w*1000000.0, grouping=True))
                
                increment += 0.05
                
            #ax.set_title('Attempted Update Rate: %s%% per second' %((rates[dir][i]*100)/size_db[dir]),
            #                  fontsize=8, fontstyle='italic')
            
            ax.get_xaxis().set_visible(False)
            step = int(time[-1]/5)
            xticks = range(0,int(time[-1]),step)
            xtickslabels = [str(int(x_value/1000000000)) for x_value in range(0,int(time[-1]),step)]
            if str(int(time[-1])/1000000000) not in xtickslabels:
                xticks.append(int(time[-1]))
                xtickslabels.append(str(int(time[-1])/1000000000))
            #ax.set_xticks(xticks)
            #ax.set_xticklabels(xtickslabels)
    
            #print range(0,int(time[-1]),50000000000)
    
            ax.tick_params(labelsize=12)
            #ax.set_ylim(0,43.0)
            #ax.set_ylim(0,float(max(max_sizes)*1.3))
            if (agg == "64"):
                ax.set_ylim(0, 2.5)
                ax.set_yticks([0,1,2])
            else:
                ax.set_ylim(0, 3.5)
                ax.set_yticks([0,1,2,3])
            ax.set_xlim(0,time[-1]+(step/5))
            #ax[i,j].set_xlim(-10,max(max_times) + 10)
            ax.grid(b=True,axis='y')
            ax.set_axisbelow(True)
            ax.set_title("%s Analytics, Attempted Update Rate = %s op/s" %(agg,str_rates[dir][i]),
                     fontproperties=font.FontProperties(size=15,weight='bold'))
                
            fig = plt.gcf()
            fig.text(0.5, 0.055, xlabel, ha='center', va='center',
                     fontproperties=font.FontProperties(size=15,weight='bold'))

            if (rates[dir][i] == 10000):
                ax.yaxis.set_tick_params(labelsize=14)
                if (agg == "64"):
                    fig.text(0.075, 0.5, ylabel, ha='center', va='center', rotation='vertical',
                             fontproperties=font.FontProperties(size=15,weight='bold'))
                    ax.set_yticklabels(['0.1','1','10'])
                else:
                    fig.text(0.060, 0.5, ylabel, ha='center', va='center', rotation='vertical',
                             fontproperties=font.FontProperties(size=15,weight='bold'))
                    ax.set_yticklabels(['0.1','1','10','100'])
            else:
                ax.set_yticklabels([])
                
            plt.savefig('queue_size/queue_size_' + dir + '_' + str(rates[dir][i]) + '_' + agg + '_mongodb_log.png', bbox_inches='tight')
            plt.clf()
            
            #f = open('queue_size/queue_size_' + dir + '_' + str(rates[dir][i]) + '_' + agg + '_mongodb.out', 'w')
            #f.write(text)
            #f.close()
