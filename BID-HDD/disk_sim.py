
# Program 2. disk_sim.py : Simulates the disk access time and returns it.
# Author: Pratik Mishra, Iowa State University.
# Compile : python disk_sim.py disk_config
# This takes input from block_io_sim.py which is LBA, operation(r/w),size(sectors))
# It has the disk geometry stored, with the current location of the head.
# It calculates the time required to move from current location to the requested location 
# returns this value to blockio_sim.py 
''' This module simulates the working of different storage devices, such as HDDs, Storage Class Memories SCMs (SSDs) deployed in Data-Centers. It simulates the devices including rotational latency, command processing, settle time, cylinder switch time, head switch time, etc. HDDs are simulated to perfection for 48 bit LBA CHS compliance.
Use of this code or part of this code, needs to be cited. Any uncited or improper usage would be dealt with copywrite compliance laws.'''



import os
import sys
import operator
import csv
import itertools
import math

from itertools import groupby
from sys import argv
from operator import itemgetter, attrgetter, methodcaller

#Global Parameters
disk_number = []
disk_type = []
sector_size = 512 
# HDD parameters
disk_number_hdd = []
number_of_cylinders = []
number_of_heads = []
sectors_per_track = []
capacity_of_hdd = []          # required for processing when CHS normally fails. We make CHS work, by keeping number of heads to maximum to 16  
                              # (therefore number of surfaces), number of sectors/track also constant to 63.
command_processing_time_list = []
settle_time_list = []
rotational_latency_list = []
average_seek_time_list = []
cylinder_switch_time_list = []
head_switch_time_list = []                              
track_to_adjacent_track_list = []
                                       
# SSD parameters
disk_number_ssd = []
number_of_pages_per_block = []
size_of_each_page = []
capacity_of_ssd = []
number_of_blocks = []            # calculated below
counter_full = []
ssd_entry = []
seek_time_ssd = []
seek_time_ssdr = []
## End of Global Parameters

# this returns the cylinder and  head it is catering for that particular lba.
# Additionally, it corrects to CHS to accomodate LBA48 for large disks.


# for checking if the number is an integer, for stripping off headers or description
def isInte(s):
	try:
            	int(s)
                return True
        except ValueError:
                return False
 
 
# making CHS compliant for 48 bit LBA by increasing number of tracks                
def LBA48Correction(disk_size):
        cylinders = int(disk_size/(63*16*sector_size)) # number of cylinders or tracks per surface
                                               # sectors/track = 63, number of heads = 16, size of each sectos (in bytes) = 512
        return cylinders 
        
                      

def read_file(config_file):
        
                                               
        counter_hdd = 0
        counter_ssd = 0
        counter = 0
        with open(config_file,'r') as config_file:
                for line in config_file:
                        l = line.strip()        # remove whitespaces from beginning and end
                        store_line = l.split(',')  # split a line according to ',' as CSV is comma separated 
                        
                        
                        try:
                                if (isInte(store_line[0]) == 1): # for beginning of integers to strip off the headers.
                                        
                                        disk_number.append(int(store_line[0]))
                                        disk_type.append(store_line[1])
                                        counter = counter_hdd + counter_ssd 
                                        if (store_line[1]=='hdd'):                                                
                                                if (int(store_line[3])>16):                                                        
                                                        store_line[3] = 16      # number of heads
                                                        store_line[4] = 63      # number of sectors/track
                                                        # we keep both the number of head and sectors/track constant. Hence increased #of tracks/cylinders
                                                        # assuming that the areal density has increased over time. Number of platters = number of heads/2. 
                                                        # number of heads = number of surfaces.
                                                        store_line[2]=LBA48Correction(int(store_line[5]))
                                                        #print store_line[2]
                                                        number_of_cylinders.append(store_line[2])                                                       
                                                        number_of_heads.append(store_line[3])
                                                        sectors_per_track.append(store_line[4])
                                                else:
                                                        
                                                        number_of_cylinders.append(int(store_line[2]))
                                                        number_of_heads.append(int(store_line[3]))
                                                        sectors_per_track.append(int(store_line[4]))                                                
                                                counter_hdd = counter_hdd + 1
                                                counter_full.append(counter) 
                                                disk_number_hdd.append(int(store_line[0]))       
                                                capacity_of_hdd.append(int(store_line[5]))
                                                
                                                command_processing_time_list.append(float(store_line[6]))
                                                settle_time_list.append(float(store_line[7]))
                                                rotational_latency_list.append(float(store_line[8]))
                                                average_seek_time_list.append(float(store_line[9]))
                                                cylinder_switch_time_list.append(float(store_line[10]))
                                                head_switch_time_list.append(float(store_line[11]))                              
                                                track_to_adjacent_track_list.append(float(store_line[12]))
                                        else:   # ssd
                                                          
                                                number_of_pages_per_block.append(int(store_line[2]))
                                                size_of_each_page.append(int(store_line[3]))
                                                capacity_of_ssd.append(int(store_line[4]))
                                                blocks_of_ssd = (int(store_line[4])/(int(store_line[3])*int(store_line[2])))
                                                number_of_blocks.append(blocks_of_ssd)
                                                seek_time_ssd.append(float(store_line[5]))  # writes
                                                seek_time_ssdr.append(float(store_line[6])) # reads
                                                disk_number_ssd.append(int(store_line[0]))
                                                counter_ssd = counter_ssd + 1
                                                counter_full.append(counter)
                        except IndexError:
                                continue
                
        
        config_file.close()
        
        #print "Counter Full:%s"%counter_full
        #print "Number of Cylinders:%s"%number_of_cylinders
        #print "Heads:%s"%number_of_heads
        #print "Capacity of hdd:%s"%capacity_of_hdd
        #print "Capcity of SSD:%s"%capacity_of_ssd
        #print "HDD:%s"%disk_number_hdd
        #print "SSD:%s"%disk_number_ssd

def mapping_hdd(bios,disk_no,requested_lba,operation,size,current_head_lba,config_file):
                 
                 read_file(config_file)
                 # access time : http://faculty.plattsburgh.edu/jan.plaza/teaching/papers/seektime.html
                 # size is in number of sectors
                 # seek_time: http://faculty.plattsburgh.edu/jan.plaza/teaching/papers/seektime.html
                 # if the request size is really large to fill multiple cylinders we also need to cover the cylinder switch time, head switch time
                 # Access time:http://pcguide.com/ref/hdd/perf/perf/spec/pos_Access.htm
                 # Sustained Transfer Rate: http://pcguide.com/ref/hdd/perf/perf/spec/trans_STR.htm
                 disk_index = disk_number_hdd.index(int(disk_no))   # for finding out disk geometry of the intended disk
                 #a good rule of thumb is that the average seek time for writes on a hard disk is about 1 millisecond higher (slower) than the specification for reads. 
                 #http://pcguide.com/ref/hdd/perf/perf/issues_ReadWrite.htm
                 
                 
                 #cylinder_total = 484514
                 #heads_total = 16
                 #sectors_total_per_track = 63
                 cylinder_total = number_of_cylinders[disk_index]
                 heads_total = number_of_heads[disk_index]
                 sectors_total_per_track = sectors_per_track[disk_index]
                 
     
                 command_processing_time = command_processing_time_list[disk_index] 
                 settle_time = settle_time_list[disk_index]
                 rotational_latency = rotational_latency_list[disk_index]       # 5.6ms for 5400 rpm disk
                 average_seek_time = average_seek_time_list[disk_index]         # in ms http://pcguide.com/ref/hdd/perf/perf/spec/posSeek-c.html
                 cylinder_switch_time = cylinder_switch_time_list[disk_index] # also known as track switch time from wikipedia in mseconds
                 #head_switch_time = 2 # switching between heads within a cylinder still requires a certain amount of time, called the head switch time
                 head_switch_time = head_switch_time_list[disk_index] # half or 1/3rd of settling time
                 track_to_adjacent_track = track_to_adjacent_track_list[disk_index] # from wikipedia and http://pcguide.com/ref/hdd/perf/perf/spec/posSeek-c.html
                 # head switch time http://pcguide.com/ref/hdd/perf/perf/spec/transHeadSwitch-c.html from wikipedia in mseconds
                 
                 # Current head location configurations
                 cylinder_current = int(math.ceil(float(current_head_lba)/(heads_total*sectors_total_per_track)))
                 # where the head is located previously                 
                 head_current = int(math.ceil(float(current_head_lba)/(sectors_total_per_track))%heads_total)
                 
                 
                 # Requested lba configurations
                 cylinder_requested_start = int(math.ceil(float(requested_lba)/(heads_total*sectors_total_per_track))) # track
                 head_requested_start = int(math.ceil(float(requested_lba)/(sectors_total_per_track))%heads_total) # surface number 
                 
                 # Access Time = Command Processing time + seek time +  settle time + rotational latency
                 
                 # 1.Command Processing time = 0.5 ms as it is the time needed for making the disk ready to process any command.
                 
                 total_command_processing_time = command_processing_time
                 
                 # 2.settle time = 0.1 ms time needed by the disk to settle after movement of head
                 # also governs the total time whenever the head moves
                 
                 location_of_req_in_track = requested_lba % sectors_total_per_track # where a request lies in a track.
                 
                 end_lba = requested_lba + size
                 end_lba_cylinder = int(math.ceil(float(end_lba)/(heads_total*sectors_total_per_track))) #cylinder number of last lba
                 number_of_head_movements = abs(int(math.ceil(end_lba/sectors_total_per_track) -math.ceil(requested_lba/sectors_total_per_track))) 
                 # it is the number of head swapping time
                 total_settle_time = settle_time + (settle_time*(number_of_head_movements))
                        
                 # 3. latency : After the actuator assembly has completed its seek to the correct track, the drive must wait for 
                 #           the correct sector to come around to where the read/write heads are located. This time is called latency.
                 # average half latency is 5.6 ms for 5400 rpm disk.
                 
                 number_of_track_changes =  number_of_head_movements               # track = cylinders of a surface
                 #total_rotational_latency = rotational_latency + (rotational_latency*(number_of_track_changes))
                 total_rotational_latency = rotational_latency
                 #print "Number of Head Movements:%d" % number_of_track_changes
                 #print "Total Rotational Latency:%f" % total_rotational_latency
                 # For total latency: for every seek add 5.6 ms plus for large requests 5.6 x number of track changes.
                 
                 # 4.seek time: seek time of a hard disk measures the amount of time required for the read/write heads to move between 
                 #              tracks over the surfaces of the platters.
                 # We have used a simple technique to calculate seek time for large transfers.
                 
                 # total time spent on head switching
                 total_head_switch_time = head_switch_time*number_of_head_movements
                 #print "Total head switch time:%f" %total_head_switch_time
                 
                 current_to_requested_cylinder_distance = abs(cylinder_current-cylinder_requested_start) # track distance
                 number_of_cylinders_filled = abs(cylinder_requested_start-end_lba_cylinder)# number of cylinders filled in one request 
                 if (current_to_requested_cylinder_distance>0):
                        cylinder_switch = 1                       # if there is a switch in current track to requested track
                 else:
                        cylinder_switch = 0
                 
                 cylinder_swapping_time = (cylinder_switch + number_of_cylinders_filled)*cylinder_switch_time
                 #print "Total cylinder_swapping_time:%f" %cylinder_swapping_time
                 # number of tracks in a platter = number of cylinders in a disk
                 track_to_track_seek_time = 0
                 if current_to_requested_cylinder_distance > 0:
                        #track_to_track_seek_time = track_to_adjacent_track
                 #else:
                        normalized_track_switch = cylinder_switch_time + ((average_seek_time- cylinder_switch_time)*float(float(current_to_requested_cylinder_distance-1)/(cylinder_total-1)))
                        track_to_track_seek_time = track_to_adjacent_track + normalized_track_switch  # slice the time taken curve into equal number of tracks apart 
                        #print "normalized_track_switch:%f" %normalized_track_switch
                 #seek_time = float(number_of_cylinders*12.0/cylinder_total) #avg_seek_time = 12ms
                        
                 
                 total_seek_time = total_head_switch_time + cylinder_swapping_time + track_to_track_seek_time
                 #print "Total Seek Time:%f" %total_seek_time
                 
                 access_time = float(total_command_processing_time) + float(total_settle_time) + float(total_rotational_latency) + float(total_seek_time)
                 
                 #print "Access Time in ms:%f" %access_time
                 return access_time,cylinder_requested_start,current_to_requested_cylinder_distance
                 
def mapping_ssd(bios,disk_no,requested_lba,operation,size,config_file):
        read_file(config_file)
        #print disk_number_ssd
		time_r = 0.0
		time_w = 0.0
        disk_index = disk_number_ssd.index(int(disk_no))
        page_size = size_of_each_page[disk_index]
        ssd_size = capacity_of_ssd[disk_index]        
        number_of_pages_needed = int(size*sector_size/page_size)       
        seek_time = seek_time_ssd[disk_index]  # http://www.samsung.com/global/business/semiconductor/minisite/SSD/global/html/whitepaper/whitepaper01.html
		seek_time_r = seek_time_ssdr[disk_index] # http://ieeexplore.ieee.org/document/7120149/
        if operation == 'R':
			time_r = float(seek_time_r)*number_of_pages_needed
		else:
			time_w =float(seek_time)*number_of_pages_needed
		time_needed = time_r + time_w
        #print "SSD Time:%s"%time_needed
        return time_needed


# For testing only, takes the input as command_file#############
'''def operations(command_file):
        with open(command_file,'r') as command_file:
                for line in command_file:
                        l = line.strip()        # remove whitespaces from beginning and end
                        store_line = l.split(',')  # split a line according to ',' as CSV is comma separated
                        if int(store_line[0]) in disk_number_hdd:
                                mapping_hdd(int(store_line[0]),int(store_line[1]),store_line[2],int(store_line[3]),int(store_line[4])) 
                        elif int(store_line[0]) in disk_number_ssd:
                                mapping_ssd(int(store_line[0]),int(store_line[1]),store_line[2],int(store_line[3]))
                        else:
                                print "NOT VALID ENTRY"        

        command_file.close()  ''' 
##############                     
#def main():

        #read_file(sys.argv[1])
        #operations(sys.argv[2]) # For testing
        
        #mapping_hdd(1,301959448,'w',1024,301958424)
        #mapping_ssd(2,201959448,'w',24)
        #mapping()

#main()
