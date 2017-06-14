# -*-coding:utf-8 -*-
#
# Copyright 2015 LinkedIn Corp. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
import sys

def dict_cursor(cursor):
  print ("dict_cursor")
  print (sys.defaultencoding)
  description = [x[0] for x in cursor.description]
  for row in cursor:
    yield dict(zip(description, row))

def copy_dict_cursor(cursor):
  print ("copy_dict_cursor")
  print (sys.defaultencoding)
  result = []
  description = [x[0] for x in cursor.description]
  for row in cursor:
    print (description)
    print row
    result.append(dict(zip(description, row)))
  return result
