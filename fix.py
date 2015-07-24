__author__ = 'Ibrahim'

#   This script fixes continuity errors. For records that reverted to a previous state
#   at a later date after some intermediate changes, the SQL join does not recognize them as a separate change. Instead
#   they are considered a continuation of the original data and merged into one row with validity period that overlaps
#   intermediate change periods. This script splits them into similar records but w/ different validity dates.
#
#   Assuming data is sorted by primary key and 'valid from' date in ascending order.

import sys      # to capture command line arguments
import csv as c      # to load csv file from workflow
import fixbatch  # supplementary function for fixing continuity in batches by id
import os
from datetime import datetime, timedelta as td

start_time = datetime.now()

pk = unicode(sys.argv[1], 'utf-8')     # getting primary key from alteryx
fp = sys.argv[2]     # getting file path from alteryx
op = sys.argv[3]     # getting output file path
df = unicode(sys.argv[4], 'utf-8')     # getting 'date from' field name
dt = unicode(sys.argv[5], 'utf-8')     # getting 'date to' field name
if len(sys.argv) >= 7:
    tformat = sys.argv[6]                      # getting date format
else:
    tformat = '%Y-%m-%d'
if len(sys.argv) == 8:
    exec('offset = td(' + sys.argv[7].replace(')', '') + ')')  # checking if offset present in dates
else:
    offset = td(days=1)                # else setting default offset
print("Primary key is: " + pk)
print("File path is: " + fp)
print("File output path is: " + op)

timestamp = datetime.now().strftime("%Y_%m_%d (%H %M %S)")

f = open(fp, 'rb')      # opening file for reading data
o = open(op, 'wb')      # opening file for writing output
if fp.rfind('\\') == -1:
    directory = 'logs'
else:
    directory = fp[:fp.rfind('\\')]+'\\logs'
if not os.path.exists(directory):
    os.makedirs(directory)
l = open(directory + '\\' + timestamp + '.txt', 'wb')   # opening log file.
r = c.reader(f)       # passing to csv reader
w = c.writer(o)       # passing output file to csv writer

header = r.next()       # obtaining header info
header = [x.decode('utf-8-sig') for x in header]    # removing BOM characters from unicode (something I discovered)
w.writerow(header)     # writing header to output
pki = header.index(pk)  # getting primary key index from header
dfi = header.index(df)
dti = header.index(dt)
count = 0               # setting count variable to count processed rows
totalfixcount = 0       # total count of fixes

currRows = [r.next()]
currId = currRows[0][pki]
cond = True             # setting loop condition to true

while cond:
    try:
        nextRow = r.next()               # reading row
    except Exception:
        cond = False
    if nextRow[pki] != currId or cond is False:       # if next row has different primary key than current row batch
        if len(currRows) > 1:
            result, c = fixbatch.fixit(currRows, dfi, dti, tformat, offset)   # calling fix function on batch
            totalfixcount += c               # tallying fixes done
            w.writerows(result)              # storing processed result
            if c != 0:                       # logging fixes done, if any
                print >>l, str(c) + " conflicts fixed in primary key: " + currId + "\n\r"
        else:
            # currRows = [x.decode('utf-8') for x in currRows]
            w.writerows(currRows)              # storing processed result
        count += len(currRows)
        print "\r" + str(count) + " records processed.",
        currId = nextRow[pki]            # creating next id group
        currRows = [nextRow]
        continue
    else:
        currRows.append(nextRow)         # grouping rows by id for fixing later

run_time = datetime.now() - start_time

print "\nEnd of file reached."

print >>l, "Total fixes done: " + str(totalfixcount) + "\n\r"
print >>l, "Total records processed: " + str(count) + "\n\r"
print >>l, "Timestamp: " + timestamp + "\n\r"
print >>l, "Runtime: " + str(run_time) + "\n\r"
print >>l, "Source file: " + fp
f.close()       # closing all files
o.close()
l.close()
print "Fixes done:", totalfixcount
print "Runtime:", run_time
