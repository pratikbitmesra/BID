# Program 5: pla_algorithm.py: Process level anticipation algorithm or BID-HDD: Bulk I/O Dispatch
# Authors: Pratik Mishra
# Takes into input the output of blockio_sim
# extracts process_id,lba,xfrlen,operation,timestamp from the file of state Q
# Distinguish them into SSD or HDD favorable, by process level buckets
# Compile: python pla_algorithm.py disk_config trace_file_pids.csv request_queue_length size_of_each_bio SSD_disk_number HDD_disk_number intial_wait_time percentage_of_a_bios_determining_SSD read_threshold write_threshold
# python pla_algorithm_HDD.py disk_config pid_small 128 1024 2 1 1000 20 500 500 > result

import os
import sys
import operator
import csv
import itertools
import math
import collections

import vfs_sim
import disk_sim
import blockio_sim


from itertools import groupby
from sys import argv
from operator import itemgetter, attrgetter, methodcaller
# list_of_bios_information_from_blockio_sim has bios,range_pids,range_lba,xfrlen_bios_pid,range_timestamp,operation_cluster,xfrlen_sum_till_now

# Global parameters to define for info_from_bio_structures, proc is appended for processingh


total_bio_structure = []
time_in_SSD_list = []
time_in_HDD_list = []
disk_config_file = sys.argv[1]
trace_file_name = sys.argv[2]
SSD_disk_number = int(sys.argv[5])
HDD_disk_number = int(sys.argv[6])

global processes_in_request_queue
global processes_in_anticipation_queue
global processes_in_dispatch_queue
global time_series_per_process 
global anticipation_time_per_process
global total_time_in_SSD
global process_already_in_table
global threshold_wait_per_process 

global process_already_in_table
global time_series_per_process
global anticipation_time_per_process

threshold_wait_per_process = {}
process_already_in_table = []
total_time_in_SSD = 0.0
processes_in_anticipation_queue = {} 
processes_in_request_queue = {}
processes_in_dispatch_queue = {}
anticipation_time_per_process = {}
time_series_per_process = {}

global total_time_in_HDD
global total_time_in_anticipation_HDD
global time_in_anticipation_list
global total_time_for_this_operation_list
global Final_HDD_time
global simulated_time
total_time_in_HDD = 0.0
total_time_in_anticipating_HDD = 0.0
time_in_anticipation_list = []
total_time_for_this_operation_list = []
Final_HDD_time = 0.0
# For Calculating anticipation time
initial_wait_time = int(sys.argv[7])
#wait_time_queue = collections.deque(maxlen=10)
wait_time_queue = []
weights = []
for x in xrange(0,10):
        wait_time_queue.append(initial_wait_time)
        weights.append((x+1)*initial_wait_time)
                      

#threshold_factor = int(sys.argv[7])   # total wait time for any process

percentage_of_a_bios_determining_SSD = int(sys.argv[8])
read_threshold = int(sys.argv[9])
write_threshold = int(sys.argv[10])

global total_number_of_head_movements
total_number_of_head_movements = 0

global current_head_lba
current_head_lba = 0

global time_for_complete_processing
time_for_complete_processing = 0.0

global cylinder_current
cylinder_current = []

global distance_between_cylinders
distance_between_cylinders = [] 

global simulated_time
global simulated_time_for_this_operation
simulated_time = []
simulated_time_for_this_operation = 0.0

global operation_if_read_or_write 
operation_if_read_or_write = []


global random
random = 0


def info_from_bio_structures(list_of_bios_information_from_blockio_sim):
        
        # taking structures into cases
        global bios_proc
        global range_pids_proc 
        global range_lba_proc 
        global xfrlen_bios_pid_proc 
        global range_timestamp_proc 
        global operation_cluster_proc 
        global xfrlen_sum_till_now_proc
        
        bios_proc = list_of_bios_information_from_blockio_sim[0]
        range_pids_proc = list_of_bios_information_from_blockio_sim[1]
        range_lba_proc = list_of_bios_information_from_blockio_sim[2]
        xfrlen_bios_pid_proc = list_of_bios_information_from_blockio_sim[3]
        range_timestamp_proc = list_of_bios_information_from_blockio_sim[4]
        operation_cluster_proc = list_of_bios_information_from_blockio_sim[5]
        xfrlen_sum_till_now_proc = list_of_bios_information_from_blockio_sim[6]
        
        
        #print bios_proc
#def check_if_in_ssd(bios_id,p_id,bio_vecs,xfrlen_bio_vecs,operation_cluster_proc):

# function for converging same bios field and cluster them together
def pool_bio_structure_maker():
        
        working_index = 0
        global bios_id_field
        global p_id_field
        global operation_field
        global lba_field
        global xfrlen_field
        global xfrlen_sum_till_now_field
        global timestamp_field

        bios_id_field = []
        p_id_field = []
        operation_field = []
        lba_field = []
        xfrlen_field = []
        xfrlen_sum_till_now_field = []
        timestamp_field = []
        
        cluster_bios_proc = [list(v) for k,v in itertools.groupby(bios_proc)]
        
        for x in cluster_bios_proc:                
                bios_id_field.append(x[0])
                p_id_field.append(range_pids_proc[working_index])
                operation_field.append(operation_cluster_proc[working_index])
                
                local_lbas = []                
                local_xfrlens = []
                local_xfrelen_sum_till_now = []                
                local_timestamps = []
                for y in xrange(0,len(x)):
                        local_lbas.append(range_lba_proc[working_index])
                        local_xfrlens.append(xfrlen_bios_pid_proc[working_index])
                        local_xfrelen_sum_till_now.append(xfrlen_sum_till_now_proc[working_index])                
                        local_timestamps.append(range_timestamp_proc[working_index])
                        working_index = working_index + 1 
                lba_field.append(local_lbas)
                xfrlen_field.append(local_xfrlens)
                xfrlen_sum_till_now_field.append(local_xfrelen_sum_till_now[-1])
                timestamp_field.append(local_timestamps)
        
        writing_clustered_bios(sys.argv[2]) 
        request_queue_structure()

def request_queue_structure():
        
        number_of_iterations = int(math.ceil(float(len(bios_id_field))/request_queue_length)) + 1
        starting_bios_index = 0
        ending_bios_index = 0
        for i in xrange(1,(number_of_iterations+1)):
                starting_bios_index = ending_bios_index
                if len(bios_id_field)%request_queue_length == 0:
                         ending_bios_index = (i*request_queue_length)       
                else:
                        if i == number_of_iterations:
                                ending_bios_index = len(bios_id_field)
                        else:
                                ending_bios_index = (i*request_queue_length)  
                                
                
                
                structures_in_request_queue(bios_id_field[starting_bios_index:ending_bios_index],p_id_field[starting_bios_index:ending_bios_index],operation_field[starting_bios_index:ending_bios_index],lba_field[starting_bios_index:ending_bios_index],xfrlen_field[starting_bios_index:ending_bios_index],xfrlen_sum_till_now_field[starting_bios_index:ending_bios_index],timestamp_field[starting_bios_index:ending_bios_index])
                #print bios_id_field[starting_bios_index:ending_bios_index]
        #print len(bios_id_field)
        #print number_of_iterations                                
        #print request_queue_length                           

# For bucketizing inside the request queue
def finding_unique_processes(pids_in_req_q):
        unique_processes_in_req_q = []
        for i in pids_in_req_q:
                if i not in unique_processes_in_req_q:
                        unique_processes_in_req_q.append(i)

        return unique_processes_in_req_q

# to calculate total time in SSD
def time_in_SSD(bios,SSD_disk_number,requested_lba,type_of_operation,size):
        #disk_sim.read_file(disk_config_file)  # for making the topology of the disk
        global total_time_in_SSD
        time_in_SSD_for_this_operation = disk_sim.mapping_ssd(bios,SSD_disk_number,requested_lba,type_of_operation,size,disk_config_file)
        #time_in_SSD_list
        time_in_SSD_list.append(time_in_SSD_for_this_operation)
        #disk_sim.mapping_SSD(disk_no,requested_lba,operation,size)
        total_time_in_SSD = sum(time_in_SSD_list)
        
        #print "Total Time in SSD:%f"%total_time_in_SSD


# to calculate total time in HDD
#mapping_hdd(bios,disk_no,requested_lba,operation,size,current_head_lba,config_file)
def time_in_HDD(bios,HDD_disk_number,requested_lba,operation,size,end_lba,time_from_start,operation_in_processing):
        
        global total_time_in_HDD
        global total_time_in_anticipating_HDD
        global time_in_anticipation_list
        global Final_HDD_time
        global current_head_lba
        global total_number_of_head_movements
        global total_time_for_this_operation_list
        global cylinder_current
        global distance_between_cylinders
        global simulated_time
        global simulated_time_for_this_operation
        global operation_if_read_or_write
        
        current_cylinder = disk_sim.mapping_hdd(bios,HDD_disk_number,requested_lba,operation,size,current_head_lba,disk_config_file)[1] 
        cylinder_current.append(current_cylinder)
                
        cylinder_distance = disk_sim.mapping_hdd(bios,HDD_disk_number,requested_lba,operation,size,current_head_lba,disk_config_file)[2]
        distance_between_cylinders.append(cylinder_distance)
        
        if cylinder_distance == 0 or  cylinder_distance == 1:
                total_number_of_head_movements = total_number_of_head_movements
        else:
                total_number_of_head_movements = total_number_of_head_movements + 1
        
        #HDD Processing
        time_in_HDD_for_this_operation = disk_sim.mapping_hdd(bios,HDD_disk_number,requested_lba,operation,size,current_head_lba,disk_config_file)[0]
        
        time_in_HDD_list.append(time_in_HDD_for_this_operation)
        total_time_in_HDD = sum(time_in_HDD_list)
        
        # Anticipation for HDD
        time_in_anticipation = time_from_start
        time_in_anticipation_list.append(time_from_start)
        total_time_in_anticipating_HDD = sum(time_in_anticipation_list)        
        
        # Total in HDD
        total_time_for_this_operation = time_in_anticipation + time_in_HDD_for_this_operation        
        total_time_for_this_operation_list.append(total_time_for_this_operation)        
        Final_HDD_time = sum(total_time_for_this_operation_list)
        #print "time_in_HDD_for_this_operation:%f,time_in_anticipation:%f,total_time_for_this_operation:%f,Final_HDD_time:%f"%(time_in_HDD_for_this_operation,time_in_anticipation,total_time_for_this_operation,Final_HDD_time)
        
        # Simulated time
        
        simulated_time_for_this_operation = simulated_time_for_this_operation + total_time_for_this_operation
        simulated_time.append(simulated_time_for_this_operation)
        
        operation_if_read_or_write.append(operation_in_processing)
        
        if end_lba != requested_lba + size:
                current_head_lba = end_lba
        else:
                current_head_lba = requested_lba + size

        #return Final_HDD_time

  
def calculate_anticipation_time_by_weigth(time_queue):
        sum_of_weights = sum(weights) 
        sum_of_weights_and_time = 0
        for x in xrange(0,10): 
                      sum_of_weights_and_time = sum_of_weights_and_time + (time_queue[x]*weights[x])
        anticipated_time = float(sum_of_weights_and_time)/sum_of_weights
        
        return anticipated_time
        
        
def making_buckets_of_each_process_in_anticipation_queue(anticipated_queue,request_queue):
        
        # Check if already in SSD, if yes then remove those 
        #bios,pid, operation, lba, size,timestamp,trace_file_name ,media, media_number
        #global request_queue
        global random
        looping_request_queue = request_queue.copy()
        looping_anticipated_queue = anticipated_queue.copy()
        #print "START----Anticipated Queue:%s"%anticipated_queue
        #print "START----Request Queue:%s"%request_queue
        # For checking if LBA's are already in SSD or not and if yes, then remove them as well as count the time taken for processing
        already_in_SSD = []

        for each_process in looping_request_queue:
                #print each_process
                #print request_queue[each_process]
                #print "\n"
                for tuple_number in xrange(0,len(looping_request_queue[each_process])):
                        #print "GOOD"
                        for traverse_lba in xrange(0,len(looping_request_queue[each_process][tuple_number][3])):
                               
                               
                               k = vfs_sim.storage_enquiry(looping_request_queue[each_process][tuple_number][0],looping_request_queue[each_process][tuple_number][1],looping_request_queue[each_process][tuple_number][2],looping_request_queue[each_process][tuple_number][3][traverse_lba],looping_request_queue[each_process][tuple_number][4][traverse_lba],looping_request_queue[each_process][tuple_number][6][traverse_lba],trace_file_name,request_queue_length, size_of_each_bio,'SSD',SSD_disk_number)
                               #vfs_sim.storage_enquiry(bios,pid, operation, lba, size,timestamp,trace_file_name ,media, media_number)
                               # Add the time to process in SSD as well as remove from the request queue
                               if k == 1:
                                 # Time Calculation routine
                                 time_in_SSD(request_queue[each_process][tuple_number][0],SSD_disk_number,request_queue[each_process][tuple_number][3][traverse_lba],looping_request_queue[each_process][tuple_number][2],looping_request_queue[each_process][tuple_number][4][traverse_lba])
                                 already_in_SSD.append([each_process,tuple_number,traverse_lba])

                                 
                                 '''for SSD_lba in lbas_in_SSD:
                                        if len(request_queue[each_process][tuple_number][3]) == 0 or request_queue[each_process] == []:
                                                del request_queue[each_process][tuple_number] 
                                        else:
                                                del request_queue[each_process][tuple_number][3][traverse_lba]
                                                del request_queue[each_process][tuple_number][4][traverse_lba]
                                                del request_queue[each_process][tuple_number][6][traverse_lba]'''
                                        

        for x in already_in_SSD:    
                #print "Already in SSD:%s"% request_queue[x[0]]                          
                        if x[0] in request_queue.values():
                                del request_queue[x[0]][x[1]][3][x[2]]
                                del request_queue[x[0]][x[1]][4][x[2]]
                                del request_queue[x[0]][x[1]][6][x[2]]
                        if len(request_queue[x[0]][x[1]][3]) == 0 or request_queue[x[0]] == []:
                                del request_queue[x[0]][x[1]]            



        # Create Global Table with pid, amount_of_data_written, our_calculated_time
        global process_already_in_table
        global time_series_per_process
        global anticipation_time_per_process
        
        
        # Anticipation of each process 
        remove_empty_anticipated = anticipated_queue.copy()    # remove empty values
        
        for each_process in remove_empty_anticipated:
                if remove_empty_anticipated[each_process] == []:
                        del anticipated_queue[each_process]
        
        # Intitialize time_series_per_process for each process 
        for each_process in anticipated_queue:
                        if each_process not in process_already_in_table:
                                process_already_in_table.append(each_process)
                                time_series_per_process[each_process] = [initial_wait_time]*10                                
                                anticipation_time_per_process[each_process] = calculate_anticipation_time_by_weigth(time_series_per_process[each_process])
        
        for each_process in anticipated_queue:
                if anticipated_queue[each_process] != []:
                        #print "Anticipated Queue:%s"%anticipated_queue
                        #print "each_process:%d,anticipated_queue[each_process][-1][6][-1]:%f"%(each_process,anticipated_queue[each_process][-1][6][-1])
                        last_lba_time_in_anticipated_queue =  anticipated_queue[each_process][-1][6][-1]    #[6] for timesta
                
                        
                if each_process in request_queue and request_queue[each_process] != []:
                        #print "Request Queue:%s"%request_queue
                        #print "Each_process:%d,request_queue[each_process][0][6][0]:%f"%(each_process,request_queue[each_process][0][6][0])
                        first_lba_time_in_reqeust_queue = request_queue[each_process][0][6][0]
                        inter_arrival_time = abs(first_lba_time_in_reqeust_queue-last_lba_time_in_anticipated_queue)*float(1000) # in Milliseconds
                        time_series_per_process[each_process].pop(0)        
                        time_series_per_process[each_process].append(inter_arrival_time)
                        anticipation_time_per_process[each_process] = calculate_anticipation_time_by_weigth(time_series_per_process[each_process])
                        #print "Process:%d, Inter_arrival Time: %f, Anticipation Time: %f"%(each_process,inter_arrival_time,anticipation_time_per_process[each_process])


                #else:
                        #print "Process:%d,  Anticipation Time: %f"%(each_process,anticipation_time_per_process[each_process]) 
                
                
                #print "\n"      
                #inter_arrival_time = abs(first_lba_time_in_reqeust_queue-last_lba_time_in_anticipated_queue)
                #print "Each Process:%d"%each_process
                #print "Interarrival Time:%f" %inter_arrival_time
                #print "time_series_per_process[%d]:%s"%(each_process,time_series_per_process[each_process])
                
                
                
                
        
        # Deciode on SSD and HDD now
        # Based on impact, bio structure size and number as well as inter-arival time.
        
        # If found SSD suitable then place in temporary bucket till comparing anticipated and request queue
        xfrlen_sum_anticipated = {}
        xfrlen_sum_request = {}
        xfrlen_sum_each_bios_anticipated = {}
        xfrlen_sum_each_bios_request = {}
        
        SSD_temporary = []      # temporary processes in SSD
        HDD_temporary = []      # temporary processes in HDD
        
        SSD_final = {}          # Final items to be sent to the SSD
        HDD_final = {}          # Final items to be sent to the HDD
        
        
        for each_process in anticipated_queue:
                xfrlen_sum_each_bios_anticipated[each_process] = []
                xfrlen_sum_each_bios_request[each_process] = []
                                
                for each_bios in anticipated_queue[each_process]:
                        xfrlen_sum_each_bios_anticipated[each_process].append(sum(each_bios[4]))  # sum of transfer lengths each_bios[4]
                xfrlen_sum_anticipated [each_process] = sum(xfrlen_sum_each_bios_anticipated[each_process])
                        
                if each_process in request_queue:
                        for each_bios in anticipated_queue[each_process]:
                                xfrlen_sum_each_bios_request[each_process].append(sum(each_bios[4]))
                        xfrlen_sum_request [each_process]= (sum(xfrlen_sum_each_bios_request[each_process]))
                packing_fraction = 0.0           # efficiency of filling a bios structure
                total_amount_of_sectors_transfered = 0
                number_of_bios_taken = 0
                number_of_bios_under_packing_fraction = 0
                for xfr_sum_each in xfrlen_sum_each_bios_anticipated[each_process]:
                        packing_fraction = float(xfr_sum_each)/size_of_each_bio
                        number_of_bios_taken = number_of_bios_taken + 1
                        total_amount_of_data_transfered = total_amount_of_sectors_transfered + xfr_sum_each
                        if packing_fraction <= (float(percentage_of_a_bios_determining_SSD)/100):
                                number_of_bios_under_packing_fraction = number_of_bios_under_packing_fraction + 1
                ###### For making Changes if no SSD (Commented next 6 lines including white line, added HDD_temporary.append(each_process))#######
                if   (number_of_bios_under_packing_fraction > number_of_bios_taken/2) and (total_amount_of_sectors_transfered<(float(percentage_of_a_bios_determining_SSD*size_of_each_bio)/100)): 
                        SSD_temporary.append(each_process) 
                                                    
                else:
                        HDD_temporary.append(each_process)


                
        #print "SSD temporary:%s"% SSD_temporary
        #print "HDD temporary:%s"% HDD_temporary       
 
        for each_process in SSD_temporary:
                        packing_fraction_request = 0.0
                        total_amount_of_sectors_transfered_request = 0
                        number_of_bios_taken_request = 0
                        number_of_bios_under_packing_fraction_request = 0 
                        total_amount_of_sectors_transfered_request = 0                       
                        if each_process in request_queue:    
                                for xfr_sum_each in xfrlen_sum_each_bios_request[each_process]:
                                        packing_fraction_request = float(xfr_sum_each)/size_of_each_bio
                                        number_of_bios_taken_request = number_of_bios_taken_request + 1
                                        total_amount_of_sectors_transfered_request = total_amount_of_sectors_transfered_request + xfr_sum_each
                                        if packing_fraction_request <= (float(percentage_of_a_bios_determining_SSD)/100):
                                                number_of_bios_under_packing_fraction_request = number_of_bios_under_packing_fraction_request + 1        
                                
                         
                                if  (number_of_bios_under_packing_fraction_request >  number_of_bios_taken_request/2) and (total_amount_of_sectors_transfered_request<(float(percentage_of_a_bios_determining_SSD*size_of_each_bio)/100)):
                                        counter = 0
                                        for x in xrange(0,len(request_queue[each_process])):
                                                if sum(request_queue[each_process][x][4]) < int(float(percentage_of_a_bios_determining_SSD*size_of_each_bio)/100):
                                                        counter = counter + 1
                                        if counter >= 1:
                                                SSD_final[each_process] = anticipated_queue[each_process]
                                                SSD_final[each_process] = SSD_final[each_process] + request_queue[each_process] 
                                        else:
                                                HDD_final[each_process] = anticipated_queue[each_process]
                                                HDD_final[each_process] = HDD_final[each_process] + request_queue[each_process]                                                
                                                        
                                else:
                                        HDD_final[each_process] = anticipated_queue[each_process]
                                        HDD_final[each_process] = HDD_final[each_process] + request_queue[each_process]  
                        else:
                                SSD_final[each_process] = anticipated_queue[each_process]

                        #print "SSD_final[%d]:%s"%(each_process,SSD_final[each_process])
                        #print "SSD_final:%s"%(SSD_final)
        
        # SSD Flush- flush all requests from request_queue and anticipated_queue which are SSD Favorable
        # Calculate the time for SSD accesses
        # Then delete all the anticipated_queue and request_queue
        
        #### Added for no SSD all HDD solution- COMMENT IT###
        SSD_final = []
        #####################################################
         
        for each_process in SSD_final:
                for each_tuple in SSD_final[each_process]:
                        for each_location in xrange(0,len(each_tuple[3])):
                                vfs_sim.put_in_SSD(each_tuple[0],each_tuple[1],each_tuple[2],each_tuple[3][each_location],each_tuple[4][each_location],each_tuple[6][each_location],trace_file_name,request_queue_length, size_of_each_bio,'SSD',SSD_disk_number)
                                time_in_SSD(each_tuple[0],SSD_disk_number,each_tuple[3][each_location],each_tuple[2],each_tuple[4][each_location])
                                
                del anticipated_queue[each_process]
                if each_process in request_queue:
                        del request_queue[each_process]                                
        
        # HDD Flush
        ###### For making Changes if no SSD (Commented next 3 lines (uncomment them once done),  added 7 lines including comments and white spaces#######  
        '''for each_process in HDD_temporary:
                if each_process not in HDD_final:
                        HDD_final[each_process] = anticipated_queue[each_process]'''
        #### Added for no SSD all HDD solution- COMMENT IT after use and ALSO UNCOMMENT THE ABOVE 3 lines##############
        for each_process in anticipated_queue:
                if each_process in request_queue:
                        HDD_final[each_process] = anticipated_queue[each_process] + request_queue[each_process]
                else:
                        HDD_final[each_process] = anticipated_queue[each_process] 
                        
        '''if bool(anticipated_queue) is False:
                for each_process in request_queue:
                        HDD_final[each_process] = request_queue[each_process]'''                             
        ##############################################################  
             
        Final_HDD_copy = {}
        Final_HDD_copy = HDD_final.copy()
        reqeust_queue_copy = {}
        request_queue_copy = request_queue.copy()
        anticipated_queue_copy = {}
        anticipated_queue_copy = anticipated_queue.copy()
        read_processing = {}
        write_processing = {}
        dispatch_HDD = {}
        
        for each_process in Final_HDD_copy:
                if each_process not in request_queue_copy: # cover the cases when we flush already in anticipated queue and nothing is there in the request queue 
                        
                        dispatch_HDD[each_process] = Final_HDD_copy[each_process]
                        last_time_of_entire_process = Final_HDD_copy[each_process][-1][6][-1]
                        #time_in_HDD(bios,HDD_disk_number,requested_lba,operation,size,end_lba,time_taken_from_start)
                        for each_tuple in Final_HDD_copy[each_process]:
                                time_taken_from_start = abs(each_tuple[6][0]-last_time_of_entire_process)
                                operation_in_processing_to_HDD = each_tuple[2]
                                time_in_HDD(each_tuple[0],HDD_disk_number,each_tuple[3][0],each_tuple[2],sum(each_tuple[4]),each_tuple[3][-1],time_taken_from_start,operation_in_processing_to_HDD)
                        del anticipated_queue_copy[each_process]
                        
                        #del anticipated_queue[each_process]
                else: # if there is a presence in request queue
                        
                        if (each_process in anticipated_queue_copy) and (anticipated_queue_copy[each_process] != []):
                                
                                start_time_of_process = anticipated_queue_copy[each_process][0][6][0]*float(1000)        # convert to ms
                                end_time_of_process_in_anticipated_queue = anticipated_queue_copy[each_process][-1][6][-1]*float(1000)
                                threshold = write_threshold
                                if anticipated_queue_copy[each_process][0][2] == 'R':
                                        threshold = read_threshold
                                else:
                                        threshold = write_threshold
                                
                                if abs(end_time_of_process_in_anticipated_queue - start_time_of_process) >= threshold:
                                       
                                        last_time_of_last_tuple = anticipated_queue_copy[each_process][-1][6][-1]
                                        for each_tuple in anticipated_queue_copy[each_process]:
                                                time_taken_from_start = abs(each_tuple[6][0]-last_time_of_last_tuple)
                                                operation_in_processing_to_HDD = each_tuple[2]
                                                time_in_HDD(each_tuple[0],HDD_disk_number,each_tuple[3][0],each_tuple[2],sum(each_tuple[4]),each_tuple[3][-1],time_taken_from_start,operation_in_processing_to_HDD)
                                                
                                        del anticipated_queue_copy[each_process]
                                                ## BAD Point
                                                                                             
                                else:
                                        
                                        anticipation_start_time = anticipated_queue_copy[each_process][0][6][0]
                                        anticipation_end_time =   anticipated_queue_copy[each_process][-1][6][-1] 
                                        tuple_index = 0
                                        i = 0
                                        total_time_in_anticipation = []
                                        for each_tuple in request_queue_copy[each_process]:
                                                request_start_time = each_tuple[6][0]
                                                request_end_time = each_tuple[6][-1]                                                
                                                inter_arrival_time = abs(request_start_time-anticipation_end_time)*float(1000)
                                                total_time_until_threshold = abs(request_end_time-anticipation_start_time)*float(1000)
                                                if inter_arrival_time <= anticipation_time_per_process[each_process] and total_time_until_threshold<= threshold:
                                                        
                                                        #print "inter_arrival_time:%f,anticipation_time[each_process]:%f,total_time_until_threshold:%f,threshold:%f"%(inter_arrival_time,anticipation_time_per_process[each_process],total_time_until_threshold,threshold)
                                                        
                                                        i = 1
                                                        
                                                        #print "anticipated_queue_copy:%s"%anticipated_queue_copy
                                                        #print "anticipated_queue_original:%s"%anticipated_queue
                                                        for x in xrange(0,len(anticipated_queue_copy[each_process])):
                                                                #print "len(anticipated_queue_copy[each_process]):%d"%len(anticipated_queue_copy[each_process])
                                                                #print "tuple_number:%d"%x
                                                                #print "tuple_number:%d,anticipated_queue_copy[each_process][x][6][0]:%f"%(x,anticipated_queue_copy[each_process][x][6][0])
                                                                #total_time_in_anticipation.append(abs(threshold - (abs(anticipated_queue_copy[each_process][x][6][0]-anticipation_start_time)*float(1000))))        
                                                                if x < (len(anticipated_queue[each_process])-1):
                                                                        total_time_in_anticipation.append(abs(anticipated_queue_copy[each_process][x][6][-1]-anticipated_queue_copy[each_process][x+1][6][0])*float(1000))
                                                                else:
                                                                        total_time_in_anticipation.append(0.0)
      
                                                        anticipated_queue[each_process].append(each_tuple)
                                                        #print "anticipated_queue_original1:%s"%anticipated_queue
                                                        total_time_in_anticipation.append(inter_arrival_time)
                                                        #total_time_in_anticipation.append(abs(threshold-total_time_until_threshold)) 
                                                        anticipation_end_time = each_tuple[6][-1] 
                                                        del request_queue[each_process][tuple_index] 
                                                        tuple_index = tuple_index + 1
                                                        if (each_process in request_queue) and (request_queue[each_process] == []):
                                                                del request_queue[each_process]
                                                                
                                                
                                                if i == 0:
                                                 
                                                        total_time_in_anticipation = []
                                                        for x in xrange(0,len(anticipated_queue[each_process])):
                                                                                
                                                                        #total_time_in_anticipation.append( abs(anticipated_queue_copy[each_process][x][6][0] -anticipated_queue_copy[each_process][len(anticipated_queue[each_process])-1][6][-1])*float(1000) )
                                                                if x < (len(anticipated_queue[each_process])-1):
                                                                        total_time_in_anticipation.append(abs(anticipated_queue_copy[each_process][x][6][-1]-anticipated_queue_copy[each_process][x+1][6][0])*float(1000))
                                                                else:
                                                                        total_time_in_anticipation.append(0.0)
                                        counter = 0
                                        #print "A:%s"%anticipated_queue[each_process]
                                        #print "B:%s"%request_queue[each_process]
                                        for each_tuple_for_request in anticipated_queue[each_process]:
                                                
                                                time_taken_from_start = total_time_in_anticipation[counter]
                                                operation_in_processing_to_HDD = each_tuple[2]
                                                time_in_HDD(each_tuple_for_request[0],HDD_disk_number,each_tuple_for_request[3][0],each_tuple_for_request[2],sum(each_tuple_for_request[4]),each_tuple_for_request[3][-1],time_taken_from_start,operation_in_processing_to_HDD)
                                                ### BAD POINT###
                                                #print each_tuple_for_request
                                                counter = counter + 1                                            
                                        del anticipated_queue_copy[each_process]     
                                                                                                                                                                                                                                                                                                               
                    
                                                                                                       
                if each_process in anticipated_queue:                                                                        
                        del anticipated_queue[each_process]
        
                
        #print "SSD_final:%s"%SSD_final.keys()
        #print "HDD_final:%s"%HDD_final.keys()
        #print "\n"        

        #print "HDD_Temporary:%s"%HDD_temporary                       
        #print "Anticipation Queue:%s" %anticipated_queue        
        #print "\n"
        #print " Time Series:%s"%time_series_per_process
                       

        #Determine SSD and HDD Favorable
        # First determine by amount of data written BY EACH PROCESS
        #print "STOP----Anticipated Queue:%s"%anticipated_queue
        #print "STOP----Request Queue:%s"%request_queue
        #print "\n"
        return request_queue # it should return the processes in the current to be anticipated
         
def structures_in_request_queue(bios_req, pid_req, operation_req, lba_req, xfrlen_req, xfrlen_sum_req, timestamp_req):
        global unique_processes_inside_req_queue
        
        order_of_bios = zip(bios_req, pid_req, operation_req, lba_req, xfrlen_req, xfrlen_sum_req, timestamp_req)
        unique_processes_inside_req_queue = []
        unique_processes_inside_req_queue = finding_unique_processes(pid_req)
        
        indices_of_each_process = []
        indices_of_unique_processes = []
        #global previous_slab 
        processes_in_request_queue = {}
        
        #previous_slab = process_slab
        
        for unique_pid in unique_processes_inside_req_queue:
                indices_of_each_process = [i for i, x in enumerate(pid_req) if x == unique_pid]              
                processes_in_request_queue[unique_pid] = []
                for  x in indices_of_each_process:
                        processes_in_request_queue[unique_pid].append(order_of_bios[x])
                indices_of_unique_processes.append(indices_of_each_process)
                indices_of_unique_processes = []
        number_of_unique_processes_inside_req_queue = len(unique_processes_inside_req_queue)
        global processes_in_anticipation_queue 
        #previous_slab = process_slab
        pruned_HDD_request_queue = making_buckets_of_each_process_in_anticipation_queue(processes_in_anticipation_queue,processes_in_request_queue)
               
        processes_in_anticipation_queue = pruned_HDD_request_queue
        
        
        #print unique_processes_inside_req_queue
        #print indices_of_unique_processes
        
        #print number_of_unique_processes_inside_req_queue 
        
        #for i in xrange(0,number_of_unique_processes_inside_req_queue):
        # Clusterizing each process and its values.cluster_id = unique_pid                
        # Reordering for keeping all requests of a process together.
        
        
                
        
        #indices = [i for i, x in enumerate(my_list) if x == "whatever"]
        #print lba_req

def writing_clustered_bios(trace_file_orginal):               
             
             #global bio_pack
             bios_pack = []
             bios_pack = zip(bios_id_field,p_id_field,operation_field,lba_field,xfrlen_field,xfrlen_sum_till_now_field,timestamp_field)
 
             '''with open(('BIO_STRUCTURE_'+trace_file_orginal),'w+') as test_file:
                        test_file.write("bios_id_field,p_id_field,operation_field,lba_field,xfrlen_field,xfrlen_sum_till_now_field,timestamp_field")
                        test_file.write("\n")
                        writer = csv.writer(test_file, delimiter=',')
                        writer.writerows(bios_pack)

             test_file.close()'''        
            

def main():
        global request_queue_length
        global size_of_each_bio
        global time_for_complete_processing
        global Final_HDD_time
        global total_time_in_anticipating_HDD
        global total_time_in_HDD
        global total_time_in_SSD
        size_of_each_bio = int(sys.argv[4])
        request_queue_length = int(sys.argv[3])
        # Making bio structures        
        #blockio_sim.read_trace_file(sys.argv[2])
        
        path = "RemoveThis_"+trace_file_name+"_"+str(size_of_each_bio)+"_"+str(request_queue_length)+"_directory/"
        if not os.path.exists(path):
                os.makedirs(path)
        os.system("split -l 50000" +" " + trace_file_name +" " " ./"+path+"/splitter")
        listing = os.listdir(path)
        for infile in sorted(listing):
                print "current file is: " + infile
                list_of_bios_information_from_blockio_sim=blockio_sim.making_bio_structures(int(sys.argv[4]),path+infile) # extract information from blockio_sim.py module as lists of
                                                                                               #bios,range_pids,range_lba,xfrlen_bios_pid,range_timestamp,operation_cluster,xfrlen_sum_till_now
                info_from_bio_structures(list_of_bios_information_from_blockio_sim)
        #blockio_sim.writing_bios_to_file(sys.argv[2])
                pool_bio_structure_maker()
                print "HDD Processing Time:%f,total_time_in_HDD:%f, total_time_in_anticipating_HDD:%f"%(Final_HDD_time,total_time_in_HDD,total_time_in_anticipating_HDD)
                
        os.system("rm -rf "+path)
                
        
        
        global total_time_for_this_operation_list
        global cylinder_current
        global distance_between_cylinders
        global simulated_time
        
        cylinder_and_time = []
        cylinder_and_time = sorted(zip(cylinder_current,simulated_time,operation_if_read_or_write),key=itemgetter(1),reverse=False)
        
        with open(('PLA_PureHDD_cylinder_and_time'+trace_file_name),'w+') as bin_info_file:
                bin_info_file.write("cylinder_current,simulated_time,operation_if_read_or_write")
                bin_info_file.write("\n")
                writer = csv.writer(bin_info_file, delimiter=',')
                writer.writerows(cylinder_and_time)

        bin_info_file.close()
        
        distance_cylinders_and_time = []
        distance_cylinders_and_time = sorted(zip(distance_between_cylinders,simulated_time,operation_if_read_or_write),key=itemgetter(1),reverse=False)
        with open(('PLA_PureHDD_distance_cylinders_and_time'+trace_file_name),'w+') as bin_info_file:
                bin_info_file.write("distance_between_cylinders,simulated_time,operation_if_read_or_write")
                bin_info_file.write("\n")
                writer = csv.writer(bin_info_file, delimiter=',')
                writer.writerows(distance_cylinders_and_time)

        bin_info_file.close()        
        
        cylinder_time_taken_to_process = []
        cylinder_time_taken_to_process = zip(cylinder_current,total_time_for_this_operation_list,operation_if_read_or_write)
        with open(('PLA_PureHDD_cylinder_time_taken_to_process'+trace_file_name),'w+') as bin_info_file:
                bin_info_file.write("cylinder_current,total_time_for_this_operation_list,operation_if_read_or_write")
                bin_info_file.write("\n")
                writer = csv.writer(bin_info_file, delimiter=',')
                writer.writerows(cylinder_time_taken_to_process)

        bin_info_file.close()   
              
        time_for_complete_processing = Final_HDD_time + total_time_in_SSD
        print "Total Time for Processing:%f "%time_for_complete_processing
        print "HDD Processing Time:%f,total_time_in_HDD:%f, total_time_in_anticipating_HDD:%f"%(Final_HDD_time,total_time_in_HDD,total_time_in_anticipating_HDD)
        #print "HDD Processing Time:%f,total_time_in_HDD:%f"%(Final_HDD_time,total_time_in_HDD)
        print "SSD Processing Time:%f"%total_time_in_SSD
        print "File:%s"%trace_file_name
        print "PLA: Total Number of Head Movements:%s"%total_number_of_head_movements
        print "Total Number of Cylinders travesed:%s"%sum(distance_between_cylinders)
        print "Total Processing Time: %s"% sum(total_time_for_this_operation_list)
        print "Size of each bio:%d,request queue length:%d"%(size_of_each_bio,request_queue_length)
        
main()
