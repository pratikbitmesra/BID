#!/bin/bash
#Author: Pratik Mishra



  
for filename in grepfindingthe_ randomdatawriter_ RandomTextWriter_ sort_new_random_ sortrandom_ terasort200_new_ wordcountdata_ wordstandarddeviation_
do
                pid='pid'
                file_end='.csv'
                echo "Using File:"$filename$pid$file_end
                mv /home/mishrap/PureHDD/$filename$pid$file_end ./
                echo "Moved File:"$filename$pid$file_end
                python pla_algorithm_hybrid.py disk_config $filename$pid$file_end 128 1024 2 1 1000 20 500 500 > result_pla_hybrid_128_1024_$filename$pid$file_end
                echo "Finished using the file:"$filename$pid$file_end
                mv $filename$pid$file_end /home/mishrap/PureHDD/
                echo "Kept back File:"$filename$pid$file_end

done
 
for filename in grepfindingthe_ randomdatawriter_ RandomTextWriter_ sort_new_random_ sortrandom_ terasort200_new_ wordcountdata_ wordstandarddeviation_
do
        pid='pid'
        file_end='.csv'
        starting_part_of_plotting_file='PLA_Hybrid_cylinder_and_time'
        starting_part_of_distance_file='PLA_Hybrid_distance_cylinders_and_time'
        starting_part_of_process_file='PLA_Hybrid_cylinder_time_taken_to_process' 
        ssd_file='SSD_entries_' 
        request_queue_length='_128'
        size_of_each_bios='_1024'  
        
        zip PLA_Hybrid_128_1024_500_results.zip result_pla_hybrid_128_1024_$filename$pid$file_end
        rm result_pla_hybrid_128_1024_$filename$pid$file_end
        
        zip PLA_Hybrid_128_1024_500_distance_cylinders_and_time.zip $starting_part_of_distance_file$filename$pid$file_end
        rm $starting_part_of_distance_file$filename$pid$file_end
        
        zip PLA_Hybrid_128_1024_500_cylinder_and_time.zip $starting_part_of_plotting_file$filename$pid$file_end
        rm $starting_part_of_plotting_file$filename$pid$file_end
        
        zip PLA_Hybrid_128_1024_500_cylinder_time_taken_to_process.zip $starting_part_of_process_file$filename$pid$file_end
        rm $starting_part_of_process_file$filename$pid$file_end   
        
        zip PLA_Hybrid_128_1024_500_SSD_files.zip  $ssd_file$filename$pid$file_end$request_queue_length$size_of_each_bios               


done

zip PLA_Hybrid_128_1024_500_Final_Results_Run_1 PLA_Hybrid_128_1024_500_results.zip PLA_Hybrid_128_1024_500_distance_cylinders_and_time.zip PLA_Hybrid_128_1024_500_cylinder_and_time.zip PLA_Hybrid_128_1024_500_cylinder_time_taken_to_process.zip  PLA_Hybrid_128_1024_500_SSD_files.zip
