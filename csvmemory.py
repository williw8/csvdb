#    csvmemory.py - manipulate "CSV" data in memory 
#
#    Copyright (C) 2017 Winslow Williams 
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

import csvdb

class MemoryWriter(object):


  def __init__(self):
    self.delimiter = ','
    self.comment = '#'
    self.eol = '\n'
    self.header = list()
    self.rows = list()
    self.path = ''

  def load(self,path):
    self.path = path
    rv = csvdb.CSVTable()
    rv.set_store(self)
    return rv

  def save(self,path):
    with open(path,'wb') as fd:
      idx = 0
      for label in self.header:
        fd.write(label)
        if idx == (len(self.header)-1):
          fd.write(self.eol)
        else:
          fd.write(self.delimiter)
        idx += 1
      for row in self.rows:
          idx = 0
          for v in row:
            if len(v) > 0:
              fd.write(v)
            if idx == (len(row)-1):
              fd.write(self.eol)
            else:
              fd.write(self.delimiter)
            idx += 1

  def setHeader(self,v):
    if isinstance(v,list):
      self.header = v
    else:
      raise TypeError('Header should be a list of strings')

  def appendRow(self,v):
    if isinstance(v,list):
      if len(v) == len(self.header):
        self.rows.append(v)
      else:
        raise Exception('Length of row: ' + str(len(v)) + ', Length of header: ' + str(len(self.header)))
    else:
      raise TypeError('Header should be a list of strings')

  def close(self):
    pass
