__author__ = 'Ibrahim'

from datetime import datetime as dt

#   Iterates over a batch of records to find conflicts. If the next row's valid dates are between the current row's
#   valid dates, then the current row is split to sandwich the next row. As a consequence, the from dates of two rows
#   (the split row and its next row) might me the same, which is fixed by delaying the from date of the current row.
#   As another consequence the from/to dates of the current row might be equal or not chronological in which case the
#   current row is deleted. In all cases the split row is inserted into the batch in chronological order.




def fixit(batch, dfi, dti, tformat, offset):
    count = 0               # resetting number of fixes
    cond = True             # setting loop condition to true
    i = 0                   # setting batch iterator to 0
    while cond:
        try:        # in case incoming batch has some faulty date data
            currfdate = dt.strptime(batch[i][dfi], tformat)         # assigning converted date values from batch
            currtdate = dt.strptime(batch[i][dti], tformat)
            nextfdate = dt.strptime(batch[i+1][dfi], tformat)
            nexttdate = dt.strptime(batch[i+1][dti], tformat)
        except IndexError:
            return batch, count
        if currfdate > currtdate:                                   # deleting asynchronous records
            del batch[i]
            continue
        if currfdate == nextfdate:                                  # modifying overlapping records
            batch[i][dfi] = (nexttdate + offset).strftime(tformat)  # delaying current record's from date
            temp = batch[i]                                         # swapping w/ next record to preserve order
            batch[i] = batch[i+1]
            batch[i+1] = temp
            count += 1
            continue
        if nextfdate >= currfdate and nexttdate <= currtdate:       # splitting sandwiching records
            count += 1
            dupRow = batch[i][:]                                    # making copy of current row
            newtdate = (nextfdate - offset).strftime(tformat)       # assigning dates to accommodate sandwiched rec
            newfdate = (nexttdate + offset).strftime(tformat)
            batch[i][dti] = newtdate
            dupRow[dfi] = newfdate
            placeRecord(batch, dupRow, i, dfi, tformat)              # placing record split copy in correct order
            continue
        if len(batch) - 2 <= i:                                      # exit condition
            cond = False
            continue
        else:
            i += 1
    return batch, count


def placeRecord(batch, dupRow, i, dfi, tformat):         # helper function for inserting rows in correct order
    while i < len(batch):
        if dt.strptime(batch[i][dfi], tformat) >= dt.strptime(dupRow[dfi], tformat):
            batch.insert(i, dupRow)
            return
        i += 1
    batch.insert(len(batch), dupRow)
    return
