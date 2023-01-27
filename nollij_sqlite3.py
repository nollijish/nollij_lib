#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sqlite3


# In[ ]:


def get_pri_keys(cur,table):
    # get primary keys from a table
    # input table has to be a string and is the table for which outpul is desired
    # input cur is a cursor object connected to a database
    # output tuple of primary key ordered by orig construction
    table = (table,)
    sql = '''
    SELECT name, pk 
    FROM pragma_table_info(?)
    WHERE pk > 0;
    '''
    columns = cur.execute(sql, table).fetchall()
    pri_key = tuple(i[0] for i in sorted(columns, key=(lambda x:x[1])))
    return pri_key


# In[ ]:


def get_tables(cur):
    # input cur is a cursor object connected to a database
    # output list of tables
    sql = '''
    SELECT name 
    FROM sqlite_schema 
    WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'
    ORDER BY 1;
    '''
    tables = [i[0] for i in cur.execute(sql).fetchall()]
    return tables


# In[ ]:


def get_forn_keys(cur,table):
    # get primary keys from a table
    # input table has to be a string and is the table for which outpul is desired
    # input cur is a cursor object connected to a database
    # output list of tuples of foreign keys in the form (localAttribute, foreignTable.foreignAttribute)
    table = (table,)
    sql = '''
    SELECT *
    FROM pragma_foreign_key_list(?)
    '''
    blurb = cur.execute(sql,table).fetchall()
    forn_key = [(i[3],'{}.{}'.format(i[2],i[4])) for i in blurb]
    return forn_key


# In[ ]:


def get_schema(cur,table,columns=False):
    # get full table creation schema from a database
    # input table has to be a string and is the table for which outpul is desired
    # input cur is a cursor object connected to a database
    # input columns is boolean and specifies whether only columns are returned
    # output is the schema used to build the original table
    table = (table,)
    sql = '''
    SELECT sql 
    FROM sqlite_schema
    WHERE tbl_name = ? AND type = 'table'
    ORDER BY tbl_name, type DESC, name;
    '''
    blurb = cur.execute(sql,table).fetchall()
    remove_char = [ord(i) for i in ['\n','\t','(',')',',']]
    remove_uni = [34, 39, 168, 180]
    remove = remove_char + remove_uni
    blurb2 = [i.lstrip(' ') for i in blurb[0][0].translate({i:None for i in remove}).split('\r') if i!='']
    for i,k in enumerate(blurb2):
        if k.startswith('ON DELETE'):
            blurb2[i-1] += k
            blurb2[i] = blurb2[i-1]
    schema = []
    [schema.append(i) for i in blurb2 if i not in schema]
    if columns:
        schema = [i for i in schema if (i.startswith('[') or i.startswith('CREATE'))]
    return schema

