# Program 2. blockio_sim.py : 
# Author: Pratik Mishra, Iowa State University.
# Compile : python blockio_sim.py trace_file_from_system_new_pid.csv size_of_each_bio (in sectors)
# The input file is the output from test_1.py
# Input Format: bin_number,seq_number,process_id,lba,xfrlen,operation,timestamp,state
# Funtionalities:
#  1. Preprocessing of Input to the Block Layer "Q" state.
#       1.1 Reading the trace file time base/# of requests
#       1.2  Makes each bio structure

''' Simulates the working of submitting of libaio of applications to the OS block layers. 
Forms the working of the BIO (block I/O) structures (request structure) from the bio_vec structure stages (Q) from the traces.
Use of this code or part of this code, needs to be cited. 
Any uncited or improper usage would be dealt with copyright compliance laws. Proper Citation given on the main page of BID.'''



import os
import sys
import operator
import csv
import itertools
import math
import disk_sim

from itertools import groupby
from sys import argv
from operator import itemgetter, attrgetter, methodcaller

#Global parameters
lba_list = []
p_id_list = []
xfrlen_list = []
operation_list = []
timestamp_list = []

# Bio Structure Parameters
bios = []
start_lba = []
range_lba = []
end_lba = []
range_pids = []
size_of_bio_structure = []
start_timestamp = []
range_timestamp = []
end_timestamp = []
operation_cluster = []
xfrlen_sum_till_now = []
xfrlen_bios_pid = []

# While compiling uncomment these
#file_to_be_traced = sys.argv[1]
#size_of_each_bio = int(sys.argv[2])
def read_trace_file(file_to_be_traced):

        with open(file_to_be_traced,'r') as trace_file:
                for line in trace_file.xreadlines():
                        l = line.strip()        # remove whitespaces from beginning and end
                        store_line = l.split(',')  # split a line according to ',' as CSV is comma separated 
                        
                        
                        try:
                                if (disk_sim.isInte(store_line[0]) == 1): # for beginning of integers to strip off the headers.
                                        lba_list.append(int(store_line[3]))
                                        p_id_list.append(int(store_line[2]))
                                        xfrlen_list.append(int(store_line[4]))
                                        operation_list.append(store_line[5])
                                        timestamp_list.append(float(store_line[6]))
                        except IndexError:
                                continue
        
        trace_file.close() 

def clearing_up_all_information():

        #Global parameters
        global lba_list
        global p_id_list
        global xfrlen_list
        global operation_list
        global timestamp_list
        
        lba_list = []
        p_id_list = []
        xfrlen_list = []
        operation_list = []
        timestamp_list = []

        # Bio Structure Parameters
        global bios
        global start_lba
        global range_lba
        global end_lba
        global range_pids
        global size_of_bio_structure
        global start_timestamp
        global range_timestamp
        global end_timestamp
        global operation_cluster
        global xfrlen_sum_till_now 
        global xfrlen_bios_pid        
        bios = []
        start_lba = []
        range_lba = []
        end_lba = []
        range_pids = []
        size_of_bio_structure = []
        start_timestamp = []
        range_timestamp = []
        end_timestamp = []
        operation_cluster = []
        xfrlen_sum_till_now = []
        xfrlen_bios_pid = []
               
def making_bio_structures(size_of_each_bio,file_to_be_traced):
        
        read_trace_file(file_to_be_traced)        
        cluster_pids = [list(v) for k,v in itertools.groupby(p_id_list)]
        flag_counter = 0
        intermediate_flag = 0
        element_counter = 0
        number_of_elements_in_r_w_cluster = 0
        bios_counter = 0
        counter_number_of_elements = 1
        working_index = 0
        
        for y in cluster_pids:           # first break the trace into process ids
                number_of_elements_in_clusters = len(y)     
                element_counter = element_counter + number_of_elements_in_clusters
                
                # cluster out reads/writes inside requests for a single pid
                start_pid_index_in_list = flag_counter
                end_pid_index_in_list = element_counter
                range_of_operations_in_a_pid = operation_list[start_pid_index_in_list:end_pid_index_in_list]
                cluster_reads_writes_in_pid = [list(v) for k,v in itertools.groupby(range_of_operations_in_a_pid)]
                
                #bios_counter = bios_counter + 1
                # segregating reads and writes
                for x in cluster_reads_writes_in_pid:
                        start_of_cluster_r_w = flag_counter + number_of_elements_in_r_w_cluster
                        number_of_elements_in_r_w_cluster = len(x)
                        end_of_cluster_r_w = start_of_cluster_r_w + number_of_elements_in_r_w_cluster                       
                        
                        xfrlen_sum_r_w_cluster = sum(xfrlen_list[start_of_cluster_r_w:end_of_cluster_r_w])
                
                        # for a R/W cluster do we need multiple bios to accomodate them
                        cluster_intermediate = start_of_cluster_r_w
                        xfrlen_sum_intermediate = 0
                        bios_counter = bios_counter + 1                         
                        for i in xrange(0,number_of_elements_in_r_w_cluster):
                                #working_index =  start_of_cluster_r_w + i
                                #working_index =  working_index + 1
                                #print working_index
                                if working_index <= lba_list.index(lba_list[-1]):
                                           
                                        #print lba_list[working_index]                                                   
                                        xfrlen_sum_intermediate = xfrlen_sum_intermediate + xfrlen_list[working_index]
                                        #print xfrlen_sum_intermediate
                                        if  xfrlen_sum_intermediate <= size_of_each_bio:
                                                bios.append(bios_counter)
                                                range_lba.append(lba_list[working_index])
                                                xfrlen_bios_pid.append(xfrlen_list[working_index])
                                                xfrlen_sum_till_now.append(xfrlen_sum_intermediate)
                                                range_timestamp.append(timestamp_list[working_index])
                                                range_pids.append(p_id_list[working_index])
                                                operation_cluster.append(operation_list[working_index])
                                                #working_index =  working_index + 1
                                             
                                        else:
                                                bios_counter = bios_counter + 1  
                                                bios.append(bios_counter)
                                                range_lba.append(lba_list[working_index])
                                                xfrlen_bios_pid.append(xfrlen_list[working_index])
                                                xfrlen_sum_till_now.append(xfrlen_sum_intermediate)
                                                range_timestamp.append(timestamp_list[working_index])
                                                range_pids.append(p_id_list[working_index])
                                                operation_cluster.append(operation_list[working_index])                                                     
                                                xfrlen_sum_intermediate = xfrlen_list[working_index]  
                                working_index =  working_index + 1
                        #working_index =  working_index + 1
                        #bios_counter = bios_counter + 1
                                                       

                
                flag_counter = flag_counter + number_of_elements_in_clusters        
        print "Number of Bio Structures:%d" %bios_counter
        bios_send = bios
        range_pids_send = range_pids
        range_lba_send = range_lba
        xfrlen_bios_pid_send = xfrlen_bios_pid
        range_timestamp_send = range_timestamp
        operation_cluster_send = operation_cluster
        xfrlen_sum_till_now_send = xfrlen_sum_till_now
        
        clearing_up_all_information()
        
        return bios_send,range_pids_send,range_lba_send,xfrlen_bios_pid_send,range_timestamp_send,operation_cluster_send,xfrlen_sum_till_now_send
        
        
                 
        
def writing_bios_to_file(file_to_be_traced):

        bio_pack = []
        bio_pack = zip(bios,range_pids,range_lba,xfrlen_bios_pid,range_timestamp,operation_cluster,xfrlen_sum_till_now)
                
        with open(('BIO_'+file_to_be_traced),'w+') as test_file:
                        test_file.write("bios,range_pids,range_lba,xfrlen_bios_pid,range_timestamp,operation_cluster,xfrlen_sum_till_now")
                        test_file.write("\n")
                        writer = csv.writer(test_file, delimiter=',')
                        writer.writerows(bio_pack)

        test_file.close()        

'''def main():

        #read_trace_file(file_to_be_traced)
        making_bio_structures(size_of_each_bio,file_to_be_traced)
        writing_bios_to_file(file_to_be_traced)
main()'''



