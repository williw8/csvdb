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


SELECT_STR_U = 'SELECT'
SELECT_STR_L = 'select'
WHERE_STR_U = 'WHERE'
WHERE_STR_L = 'where'
EQ = '='
LT = '<'
LTEQ = '<='
GT = '>'
GTEQ = '>='
ALL = '*'

class TableException(Exception):
  pass

class UnknownLabel(TableException):
  pass

class InvalidRow(TableException):
  pass

class InvalidRowIndex(TableException):
  pass

class InvalidSelectExpression(TableException):
  pass

class SelectExpression(object):

  def __init__(self,text,table):
    self.text = text
    self.table = table
    self.select_mnemonic = False
    self.column_expression = None
    self.columns = list()
    self.where_mnemonic = False
    self.where_name = None
    self.operator = None
    self.where_value = None

    self.select_offset = -1
    self.where_offset = -1
    self.column_expression_offset = -1
    self.where_name_offset = -1
    self.operator_offset = -1
    self.where_value_offset = -1

    if SELECT_STR_U in text:
      self.select_mnemonic = True
      self.select_offset = text.find(SELECT_STR_U)
      self.column_expression_offset = self.select_offset + len(SELECT_STR_U)
    if SELECT_STR_L in text:
      self.select_mnemonic = True
      self.select_offset = text.find(SELECT_STR_L)
      self.column_expression_offset = self.select_offset + len(SELECT_STR_L)
    if WHERE_STR_U in text:
      self.where_mnemonic = True
      self.where_offset = text.find(WHERE_STR_U)
      self.where_name_offset = self.where_offset + len(WHERE_STR_U)
    if WHERE_STR_L in text:
      self.where_mnemonic = True
      self.where_offset = text.find(WHERE_STR_L)
      self.where_name_offset = self.where_offset + len(WHERE_STR_L)

    if EQ in text:
      self.operator = EQ
      self.operator_offset = text.find(EQ)
      self.where_value_offset = self.operator_offset + len(EQ)
      self.where_value = text[self.where_value_offset:].strip()
    elif LT in text:
      self.operator = LT
      self.operator_offset = text.find(LT)
      self.where_value_offset = self.operator_offset + len(LT)
      self.where_value = text[self.where_value_offset:].strip()
    elif LTEQ in text:
      self.operator = LTEQ
      self.operator_offset = text.find(LTEQ)
      self.where_value_offset = self.operator_offset + len(LTEQ)
      self.where_value = text[self.where_value_offset:].strip()
    elif GT in text:
      self.operator = GT
      self.operator_offset = text.find(GT)
      self.where_value_offset = self.operator_offset + len(GT)
      self.where_value = text[self.where_value_offset:].strip()
    elif GTEQ in text:
      self.operator = GTEQ
      self.operator_offset = text.find(GTEQ)
      self.where_value_offset = self.operator_offset + len(GTEQ)
      self.where_value = text[self.where_value_offset:].strip()

    if self.where_mnemonic:
      self.column_expression = text[self.column_expression_offset:self.where_offset].strip()
      if self.operator_offset > self.where_name_offset:
        self.where_name = text[self.where_name_offset:self.operator_offset].strip()
      if self.where_value_offset > self.operator_offset:
        self.where_value = text[self.where_value_offset:].strip()
    else:
      self.column_expression = text[self.column_expression_offset:].strip()

    if ALL == self.column_expression:
      for col in table.getHeader():
        self.columns.append(col)
    else:
      parts = self.column_expression.split(',')
      for part in parts:
        self.columns.append(part.strip())

  def isValid(self):
    if False == self.select_mnemonic:
      return False
    if ALL != self.column_expression:
      header = self.table.getHeader()
      for col in self.columns:
        if False == (col in header):
          return False
    if self.where_mnemonic:
      if None is self.operator:
        return False
      if None is self.where_value:
        return False
    return True

  def getSelectColumns(self):
    return self.columns

  def getWhereColumn(self):
    return self.where_name

  def getText(self):
    return self.text

  def checkIntegerMatch(self,value,op):
    '''
    Returns a tuple of True/False values:
    (<is an integer>,<match value>)
    '''

    rv = (False,False)
    left = None
    right = None
    try:
      is_int = True
      left = int(value)
      right = int(self.where_value)
      if LT == op:
        result = left < right
      elif LTEQ == op:
        result = value <= self.where_value
      elif GT == op:
        result = value > self.where_value
      elif GTEQ == op:
        result = value >= self.where_value
      else:
        is_int = False
      rv = (is_int,result)
    except ValueError as ex:
      # Not an integer, return (False,False)
      pass
    return rv

  def checkFloatMatch(self,value,op):
    '''
    Returns a tuple of True/False values:
    (<is a float value>,<match value>)
    '''
    rv = (False,False)
    left = None
    right = None
    try:
      is_float = True 
      result = None
      left = float(value)
      right = float(self.where_value)
      if LT == op:
        result = left < right
      elif LTEQ == op:
        result = value <= self.where_value
      elif GT == op:
        result = value > self.where_value
      elif GTEQ == op:
        result = value >= self.where_value
      else:
        is_float = False
      rv = (is_float,result)
    except ValueError as ex:
      # Not a float value, return (False,False)
      pass
    return rv

  def checkNumericMatch(self,value,op):
    '''
    Returns a tuple of True/False values:
    (<is a numeric value>,<match value>)
    '''
    (is_numeric,result) = self.checkIntegerMatch(value,op)
    if False == is_numeric:
      (is_numeric,result) = self.checkFloatMatch(value,op)
    return (is_numeric,result)

  def checkMatch(self,value):
    rv = False
    if EQ == self.operator:
      if value == self.where_value:
        rv = True
    else:
      (is_numeric,rv) = self.checkNumericMatch(value,self.operator)
      if False == is_numeric:  
        if LT == self.operator:
          if value < self.where_value:
            rv = True
        elif LTEQ == self.operator:
          if value <= self.where_value:
            rv = True
        elif GT == self.operator:
          if value > self.where_value:
            rv = True
        elif GTEQ == self.operator:
          if value >= self.where_value:
            rv = True
    return rv 


class CSVTable:

  def __init__(self):
    self.header = list()
    self.store = None
    self.indices = dict()

  def addHeaderLabel(self,v):
    self.header.append(v)
    idx = 0
    self.indices = dict()
    for x in self.header:
      self.indices[x] = idx
      idx += 1
   
  def getHeader(self):
    return self.header

  def setStore(self,v):
    self.store = v

  def getStore(self):
    return self.store

  def close(self):
    if None is not self.store:
      self.store.close() 

  def getIter(self):
    return self.store

  def nextRow(self):
    return self.store.nextRow()

  def reset(self):
    if None is not self.store:
      self.store.close()
    return self.store.load()

  def getHeaderIndex(self,label):
    rv = -1
    if self.indices.has_key(label):
      rv = self.indices[label]
    return rv

  def getValueFromRow(self,label,row):
    rv = ''
    idx = self.getHeaderIndex(label)
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
        "columns" is either a single string, or a list of strings representing the columns to select for output.
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
        column_indices.append(self.getHeaderIndex(c))
    else:
      for c in columns:
        if c not in table.header:
          raise UnknownLabel(c)
        column_indices.append(self.getHeaderIndex(c))
    if ('*' != where_value) and (where_column not in table.header):
      raise UnknownLabel(where_column)
    if '*' != where_column:
      where_column_index = self.getHeaderIndex(where_column)
    for row in table.getIter():
      append = False
      if '*' == where_value:
        append = True
      elif where_value == row[where_column_index]:
        append = True

      if append:
        v = list()
        if '*' == columns[0]:
          rv.append(row)
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

  def selectWithSelectExpression(self,se):
    rv = list()
    if se.isValid():
      where_index = self.getHeaderIndex(se.getWhereColumn())
      column_indices = list()
      table = self.reset()
      for col in se.getSelectColumns():
        column_indices.append(self.getHeaderIndex(col))
      for row in table.getIter():
        append = False
        if where_index > -1:
          append = se.checkMatch(row[where_index])
        else:
          append = True
        if append:
          v = list()
          for i in column_indices:
            v.append(row[i])
          rv.append(v)
    else:
      raise InvalidSelectExpression(self.text)
    return rv


