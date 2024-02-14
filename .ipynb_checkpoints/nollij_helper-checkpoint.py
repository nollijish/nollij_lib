#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import csv


# In[ ]:


def write_csv(filename,data,header=''):
    # input filename is a string of the desired filename or path
    # data is information desired to be written
    # header it the first row with column information
    with open(filename,'w') as out:
        csv_out=csv.writer(out)
        csv_out.writerow(header)
        for row in data:
            csv_out.writerow(row)

