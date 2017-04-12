# Program 4: vfs_sim.py: Stores which lba is stored where
# Authors: Pratik Mishra, Iowa State University
# Takes the pid, lba, sectors, media and stores the access history
# in a file known as File System location SSD_entries_trace_file
''' This code simulates the working of the Virtual File System and VFS-SSD table. Use of this code or part of this code, needs to be cited. Any uncited or improper usage would be dealt with copywrite compliance laws.'''

import os
import sys
import operator
import csv
import itertools
import math

from itertools import groupby
from sys import argv
from operator import itemgetter, attrgetter, methodcaller



# Global storage parameters

bios_list = []
pid_list = []
start_lba_list = []
end_lba_list = []
range_lba_list = []
timestamp_list = []

#bios_req, pid_req, operation_req, lba_req, xfrlen_req, xfrlen_sum_req, timestamp_req

def put_in_SSD(bios,pid, operation, lba, size, timestamp, trace_file_name ,request_queue_length, size_of_each_bio,media, media_number):
        
        if (operation != 'R'):  # If the operation is a Write type
                with open('SSD_entries_'+trace_file_name+'_'+str(request_queue_length)+'_'+str(size_of_each_bio),'a+') as SSD_file:
                        
                        bios_list.append(bios)
                        pid_list.append(pid)
                        start_lba_list.append(lba)                        
                        end_lba = lba + size
                        end_lba_list.append(end_lba)
                        for i in xrange(size+1):
                                range_lba_list.append(lba+i)
                        timestamp_list.append(timestamp)
                        formation = str(bios)+","+str(pid)+","+str(lba)+","+str(size)+","+str(end_lba)+","+str(media)+","+str(media_number)+","+str(timestamp)
                        SSD_file.write(formation)
                        SSD_file.write("\n")
                
                
                                                        
                SSD_file.close()                        

        else:
                with open('SSD_entries_'+trace_file_name,'a+') as SSD_file: 
                        if lba in range_lba_list:
                                media_enquiry = 1
                        else:
                                media_enquiry = 0                
                                     
                              
                
                SSD_file.close()
                return media_enquiry
                
def storage_enquiry(bios,pid, operation, lba, size, timestamp, trace_file_name ,request_queue_length, size_of_each_bio, media, media_number):
        with open('SSD_entries_'+trace_file_name+'_'+str(request_queue_length)+'_'+str(size_of_each_bio),'a+') as SSD_file: 
                        if lba in range_lba_list:
                                media_enquiry = 1       # In SSD
                        else:
                                media_enquiry = 0       # In HDD
        
        SSD_file.close()
        return media_enquiry
        
#def main():

        #storage(109,15908420,5,'W','SSD',3,0.879)
        #storage(19,763109231,5,'W','SSD',3,0.973947)
        #r = storage(19,763109231,5,'R','SSD',3,0.973947)
        #s = storage(19,7631031231,5,'R','HDD',3,0.973947)
        #print r,s
#main()




