# Find Standard Deviation for CFQ till a line
# Extract all files again as GnuMetrics destroys the formation
# python stddev_calculator.py trace_file_to_be_read line_number

import os
import sys
import operator
import csv
import itertools
import math


from itertools import groupby
from sys import argv
from operator import itemgetter, attrgetter, methodcaller

global time_read
global time_write
time_read = []
time_write = []
operation_read = []
operation_write = []

trace_file_to_be_read = sys.argv[1] 
line_number = int(sys.argv[2])

def isInte(s):
	try:
            	int(s)
                return True
        except ValueError:
                return False

def read_trace():
        counter = 0
        global time_read
        global time_write
        with open(trace_file_to_be_read,'r') as trace_file:
                for line in trace_file.xreadlines():
                                if counter <= line_number:
                                        l = line.strip()        # remove whitespaces from beginning and end
                                        store_line = l.split(',')  # split a line according to ',' as CSV is comma separated 
                                        
                                        counter = counter + 1
                                        try:        
                                                
                                                if (isInte(store_line[0]) == 1): # for beginning of integers to strip off the headers.

                                                                  
                                                                                                                        
                                                                        if(store_line[2]=='R'):                                                                
                                                                                operation_read.append(store_line[2])
                                                                                time_read.append(float(store_line[1]))  
                                                                        else:
                                                                                operation_write.append(store_line[2])
                                                                                time_write.append(float(store_line[1]))               
                                                                        
                                        except IndexError:
                                                continue 

        trace_file.close() 



def average(s):
        return sum(s)*1.0/len(s)
        
              
                
def standard_deviation_calculator(time_from_i_to_d):
                avg = average(time_from_i_to_d) 
                variance = map(lambda x: (x-avg)**2, time_from_i_to_d)
                standard_deviation = math.sqrt(average(variance))

                return avg,standard_deviation
        
def main():
        global time_read
        global time_write
        read_trace()
        
        read_values = standard_deviation_calculator(time_read)
        read_mean = read_values[0]
        read_standard_deviation = read_values[1]
        
        final_read = read_mean + read_standard_deviation
        
        
        write_values = standard_deviation_calculator(time_write)
        write_mean = write_values[0]
        write_standard_deviation = write_values[1]  
        
        final_write = write_mean + write_standard_deviation
        
        with open("CFQ_Standard_Deviation",'a+') as CFQ_std_file:
                formation = str(read_mean)+","+str(read_standard_deviation)+","+str(final_read)+","+str(write_mean)+","+str(write_standard_deviation)+","+str(final_write)+","+str(line_number)+","+str(trace_file_to_be_read)
                CFQ_std_file.write(formation)
                CFQ_std_file.write("\n")
        CFQ_std_file.close()
        
              

main()

