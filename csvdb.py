#    cvsdb.py - manage CSV files kind of like database tables, kind of
#
#    Copyright (C) 2016 Winslow Williams 
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.

class UnknownLabel(Exception):
  pass

class InvalidRow(Exception):
  pass

class InvalidRowIndex(Exception):
  pass

class CSVTable:

  def __init__(self):
    self.header = list()
    self.store = None
    self.indices = dict()

  def add_header_label(self,v):
    self.header.append(v)
    idx = 0
    self.indices = dict()
    for x in self.header:
      self.indices[x] = idx
      idx += 1
   
  def get_header(self):
    return self.header

  def set_store(self,v):
    self.store = v

  def get_store(self):
    return self.store

  def close(self):
    if None is not self.store:
      self.store.close() 

  def get_iter(self):
    return self.store

  def next_row(self):
    return self.store.next_row()

  def reset(self):
    if None is not self.store:
      self.store.close()
    return self.store.load()

  def get_header_index(self,label):
    rv = -1
    if self.indices.has_key(label):
      rv = self.indices[label]
    return rv

  def get_value_from_row(self,label,row):
    rv = ''
    idx = self.get_header_index(label)
    if idx >= 0:
      if idx < len(row):
        try:
          rv = row[idx]
        except TypeError as tex:
          raise InvalidRow('Expected a list type, got the following: ' + str(type(row)))
      else:
        raise InvalidRowIndex(str(idx))
    else:
      raise UnknownLabel(label)
    return rv
 
  # FIXME: Make this a whatchmacallit that iterates one row at a time... 
  def select(self,columns,where_column,where_value):
    '''
        "columns" is either a single string, or a list of strings
        set "columns" = "*" to select all columns
        set "where_value" = "*" to select all rows. In this case 
          "where_column" doesn't matter
    '''
    rv = list()
    column_indices = list()
    table = self.reset()
    if isinstance(columns,str):
      columns = [columns]

    if '*' == columns[0]:
      for c in table.header:
        column_indices.append(self.get_header_index(c))
    else:
      for c in columns:
        if c not in table.header:
          raise UnknownLabel(c)
        column_indices.append(self.get_header_index(c))
    if ('*' != where_value) and (where_column not in table.header):
      raise UnknownLabel(where_column)
    if '*' != where_column:
      where_column_index = self.get_header_index(where_column)
    for row in table.get_iter():
      append = False
      if '*' == where_value:
        append = True
      elif where_value == row[where_column_index]:
        append = True

      if append:
        v = list()
        if '*' == columns[0]:
          v.append(row[i])
        else:
          for i in column_indices:
            v.append(row[i])
        rv.append(v)
    return rv

  def makeSingleSelectionDistinct(self,selection_list):
    rv = list()
    for x in selection_list:
      value = x[0]
      if value not in rv:
        rv.append(value)
    return rv  
