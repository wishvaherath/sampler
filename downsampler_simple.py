from __future__ import division
from itertools import izip
import gzip
import sys
import io
import random

#constants
RESOLUTION = 100
ENTRIES_TO_BATCH = 100
BUFFER = True

gz_file_name_1 = sys.argv[1].strip()
gz_file_name_2 = sys.argv[2].strip()
total_size = int(sys.argv[3].strip())
sample_size = int(sys.argv[4].strip())
ENTRIES_TO_BATCH = int(sys.argv[5].strip())
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
select_ratio = round(sample_size/total_size,int(RESOLUTION/10)) * RESOLUTION
select_ratio = select_ratio+1

def pick_reads():
    dice = random.randint(0,RESOLUTION)
    if dice < select_ratio:
        dice2 = random.randint(0,ENTRIES_TO_BATCH)
        if dice2 == 1:
            #pick
            return True
        else:
            return False
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
                message = "Picked {} entries out of {}. pick_rate: {}% , pick_progress: {}% , current_state: {}%".format(picked_count, total_count, round((picked_count/total_count) * 100,2), round((picked_count/sample_size) * 100,2), round((total_count / total_size) * 100,2))
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



