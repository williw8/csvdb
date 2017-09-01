#    csvfile.py - read/write a CSV file, not currently thread safe
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

import os
import csvdb

DOUBLE_QUOTE = '"'

class SingleFileReader(object):

  def __init__(self,path):
    self.file = None
    self.path = path
    self.delimiter = ','
    self.comment = '#'
    self.eol = '\n'
    self.error = ''

  def delimit(self,s):
    rv = list()

    seeking_resolution = None
    part = ''
    for c in s:
      if DOUBLE_QUOTE == c: 
        if DOUBLE_QUOTE == seeking_resolution:
          seeking_resolution = None  
        else:
          seeking_resolution = DOUBLE_QUOTE 
        part += c
      elif self.delimiter == c: 
        if None is seeking_resolution:
          rv.append(part)
          part = ''
        else:
          part += c
      else:
        part += c
    if len(part) > 0:
      rv.append(part)
      part = ''
    return rv

  def load(self):
    rv = csvdb.CSVTable()
    rv.setStore(self)
    try: 
      self.file = open(self.path,'rb')
      while True:
        line = self.file.readline()
        if 0 == len(line):
          break
        if line.startswith(self.comment) or line.startswith(self.eol):
          continue
        # parts = line.split(self.delimiter)
        parts = self.delimit(line)
        for part in parts:
          rv.addHeaderLabel(part.strip())
        break
    except IOError as ioe:
      self.error = 'IOError: ' +  os.strerror(ioe.errno)
    except Exception as ex:
      self.error = ex.message
    return rv

  # FIXME: This isn't thread safe, we should put a lock around the file?
  def save(self,path):
    if path == self.path:
      raise Exception("Can't write over myself")
    else:
      where = self.file.tell()
      table = self.load()
      with open(path,'wb') as fout:
        idx = 0
        for label in table.header:
          fout.write(label)
          if idx == (len(table.header)-1):
            fout.write(self.eol)
          else:
            fout.write(self.delimiter)
          idx += 1
        for row in table.getIter():
            idx = 0
            for v in row:
              if len(v) > 0:
                fout.write(v)
              if idx == (len(row)-1):
                fout.write(self.eol)
              else:
                fout.write(self.delimiter)
              idx += 1
      self.file.seek(where)

  def __iter__(self):
    count = 0
    while True:
      count += 1
      line = self.file.readline()
      if 0 == len(line):
        break
      if line.startswith(self.comment) or line.startswith(self.eol):
        continue
      values = list()
      #parts = line.split(self.delimiter)
      parts = self.delimit(line)
      for part in parts:
        values.append(part.strip())
      yield values 

  def nextRow(self):
    rv = None
    while True:
      line = self.file.readline()
      if 0 == len(line):
        break
      if line.startswith(self.comment) or line.startswith(self.eol):
        continue
      values = list()
      #parts = line.split(self.delimiter)
      parts = self.delimit(line)
      for part in parts:
        values.append(part.strip())
      rv = values
      break
    return rv 

  def close(self):
    if None is not self.file:
      self.file.close()

