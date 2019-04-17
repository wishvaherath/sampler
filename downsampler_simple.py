from __future__ import division
from itertools import izip
import gzip
import math
import sys
import io
import random
import argparse


parser = argparse.ArgumentParser(description="Randomly picks chunks of neighbouring reads to downsample paired fastq.gz files")
parser.add_argument("-1", "--R1", type=str, help="R1_fastq.gz file name", required = True)
parser.add_argument("-2", "--R2", type=str, help="R2_fastq.gz file name", required = True)
parser.add_argument("-t", "--total", type=int, help = "Total number of reads in the file", required = True)
parser.add_argument("-d", "--downsample", type=int, help = "Number of reads to be included in the result file after sampling", required = True)
parser.add_argument("-s", "--bundleSize", type=int, help = "Number of neighbouring reads to be picked per iteration (default = 10000)", default=10000)
parser.add_argument("-c", "--correction", type=float, help = "Multipler for the select radio (default =1)", default = 1)
parser.add_argument("-b","--ioBuffer", type=bool, help=" Enable buffering for read and write of files", default = True)
parser.add_argument("-r","--resolution", type=int, help = "Width of the sampling (default = 100)", default =100)
args = parser.parse_args()






#constants
RESOLUTION = args.resolution
ENTRIES_TO_BATCH = args.bundleSize
BUFFER = args.ioBuffer
SELECT_RATIO_CORRECTION = args.correction

gz_file_name_1 = args.R1
gz_file_name_2 = args.R2
total_size = args.total
sample_size = args.downsample
gz_1 = gzip.open(gz_file_name_1,'rb')
gz_2 = gzip.open(gz_file_name_2,'rb')
if BUFFER:
    f_1 = io.BufferedReader(gz_1)
    f_2 = io.BufferedReader(gz_2)
else:
    f_2 = gz_1
    f_2 = gz_2


#init the output file


output_file_1 = gz_file_name_1 + "_downsampled_to_" + str(sample_size) + "_.gz"
output_file_2 = gz_file_name_2 + "_downsampled_to_" + str(sample_size) + "_.gz"
gz_out_1 = gzip.open(output_file_1, "wb")
gz_out_2 = gzip.open(output_file_2, "wb")

if BUFFER:

    f_out_1 = io.BufferedWriter(gz_out_1)
    f_out_2 = io.BufferedWriter(gz_out_2)
else:

    f_out_1 = gz_out_1
    f_out_2 = gz_out_2

#randomizing stuff
# select_ratio = round(sample_size/total_size,int(RESOLUTION/10)) * RESOLUTION
select_ratio = sample_size/total_size
if select_ratio >=1:
    #sanity check
    print "ERROR: sampling higher than the total reads"
    sys.exit(1)

#compensating
select_ratio = select_ratio * SELECT_RATIO_CORRECTION

#auto_setting the resolution
RESOLUTION = math.pow(10,abs(int(math.log10(select_ratio)))+1)
CUTOFF = int(select_ratio * RESOLUTION) + 1

print " Selct Ratio: {} , RESOLUTION:{} cutoff: {}".format(select_ratio,RESOLUTION, (CUTOFF))

print select_ratio
def pick_reads():
    dice = random.randint(0,RESOLUTION)
    if dice < CUTOFF:
        #pick
        return True
    else:
        return False

def list_of_lists_to_file(tb_1, fo_1,tb_2, fo_2):
    for i in tb_1:
        for j in i:
            fo_1.write(j)

    for i in tb_2:
        for j in i:
            fo_2.write(j)

def check_exit(pc, ss,tc,ts, temp_batch_1, temp_batch_2):
    if tc >= ts:
        print "ERROR: File read completed without completing the sampling"
        #adds the entries in thetemp_batch buffer into the output file

        list_of_lists_to_file(temp_batch_1,f_out_1, temp_batch_2, f_out_2)
        f_1.close()
        f_2.close()
        f_out_1.close()
        f_out_2.close()
        sys.exit(1)
    if pc >= ss:
        #adds the entries in thetemp_batch buffer into the output file
        list_of_lists_to_file(temp_batch_1,f_out_1, temp_batch_2, f_out_2)
        f_1.close()
        f_2.close()

        f_out_1.close()
        f_out_2.close()
        sys.exit(0)

temp_single_1 = []
temp_single_2 = []

temp_batch_1 = []
temp_batch_2 = []

picked_count = 1
total_count = -1



for line_1, line_2 in izip(f_1,f_2):
    temp_single_1.append(line_1)
    temp_single_2.append(line_2)


    if len(temp_single_1) == 4:
        #the entry has been read
        temp_batch_1.append(temp_single_1)
        temp_batch_2.append(temp_single_2)
        temp_single_1 = []
        temp_single_2 = []

        check_exit(picked_count, sample_size, total_count,total_size,temp_batch_1,temp_batch_2)
        total_count = total_count + 1

        #status update
        if total_count % 10000 == 0:
            if picked_count != 0 and total_count != 0:
                message = "Picked {} entries out of {}%. Current_Pick_Rate: {}% , Pick_Progress: {}% , Read_Progress: {}%".format(picked_count, total_count,  round((picked_count/total_count) * 100,2), round((picked_count/sample_size) * 100,2), round((total_count / total_size) * 100,2))
                sys.stdout.write(message)
                sys.stdout.flush()
                sys.stdout.write('\b' * len(message))

    if len(temp_batch_1) == ENTRIES_TO_BATCH:
        #the buffer is full, check to keep or discard
        if pick_reads():
            list_of_lists_to_file(temp_batch_1, f_out_1, temp_batch_2, f_out_2)
            picked_count = picked_count + ENTRIES_TO_BATCH
        temp_batch_1 = []
        temp_batch_2 = []



