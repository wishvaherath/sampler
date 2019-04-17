from __future__ import division
import gzip
import sys
import io
import random

#constants
RESOLUTION = 100
LINES_TO_BATCH = 100
BUFFER = True
gz_file_name = sys.argv[1].strip()
total_size = int(sys.argv[2].strip())
sample_size = int(sys.argv[3].strip())
LINES_TO_BATCH = int(sys.argv[4].strip())
gz = gzip.open(gz_file_name,'rb')
if BUFFER:
    f = io.BufferedReader(gz)
else:
    f = gz


#init the output file
output_file = gz_file_name + "_downsampled_to_" + str(sample_size) + "_.gz"
gz_out = gzip.open(output_file, "wb")
if BUFFER:
    f_out = io.BufferedWriter(gz_out)
else:
    f_out = gz_out


#randomizing stuff
select_ratio = round(sample_size/total_size,int(RESOLUTION/10)) * RESOLUTION
select_ratio = select_ratio+1

def pick_reads():
    dice = random.randint(0,RESOLUTION)
    if dice < select_ratio:
        #pick
        return True
    else:
        return False


def list_of_lists_to_file(tb, fo):
    for i in tb:
        for j in i:
            f_out.write(j)

def check_exit(pc, ss,tc,ts, temp_batch):
    if tc >= ts:
        print "ERROR: File read completed without completing the sampling"
        #adds the entries in thetemp_batch buffer into the output file
        list_of_lists_to_file(temp_batch,f_out)
        f.close()
        f_out.close()
        sys.exit(1)
    if pc >= ss:
        #adds the entries in thetemp_batch buffer into the output file
        list_of_lists_to_file(temp_batch,f_out)
        list_of_lists_to_file(temp_batch,f_out)
        f.close()
        f_out.close()
        sys.exit(0)

temp_single = []
temp_batch = []
picked_count = 0
total_count = -1
for line in f:

    if len(temp_single) == 4:

        check_exit(picked_count, sample_size, total_count,total_size,temp_batch)
        #the entry has been read
        temp_batch.append(temp_single)
        temp_single = []
        temp_single.append(line)
        total_count = total_count + 1

        #status update
        if total_count % 10000 == 0:
            if picked_count != 0 and total_count != 0:
                message = "Picked {} entries out of {}. pick: {}% , complete: {}% , read: {}%".format(picked_count, total_count, round((picked_count/total_count) * 100,2), round((picked_count/sample_size) * 100,2), round((total_count / total_size) * 100,2))
                sys.stdout.write(message)
                sys.stdout.flush()
                sys.stdout.write('\b' * len(message))
    else:
        temp_single.append(line)

    if len(temp_batch) == LINES_TO_BATCH:
        #the buffer is full, check to keep or discard
        if pick_reads():
            list_of_lists_to_file(temp_batch, f_out)
            picked_count = picked_count + LINES_TO_BATCH
        temp_batch = []



