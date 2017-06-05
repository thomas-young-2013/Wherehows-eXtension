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

import json
import csv, sys
from org.slf4j import LoggerFactory
from wherehows.common.writers import FileWriter
from wherehows.common.schemas import DatasetSchemaRecord, DatasetFieldRecord
from AvroColumnParser import AvroColumnParser

from org.python.core import codecs
codecs.setDefaultEncoding('utf-8')

class HBaseTransform:
  def __init__(this):
        this.logger = LoggerFactory.getLogger('jython script : ' + this.__class__.__name__)

  def transform(this, raw_metadata, metadata_output, field_metadata_output):
    input_json_file = open(raw_metadata, 'r')
    schema_file_writer = FileWriter(metadata_output)
    field_file_writer = FileWriter(field_metadata_output)
    i=0
    this.sort_id = 0
    o_urn = ''
    for line in input_json_file:
      try:
        j = json.loads(line)
      except:
        this.logger.error("   Invalid JSON:\n%s" % line)
        continue
      i += 1
      o_field_list_ = []
      this.sort_id = 0
      if not j.has_key('attributes'):
        o_properties = {"doc": null}
      else:
        o_properties = dict(j['attributes'].items())
        del j['attributes']
      if j.has_key('uri'):
        o_urn = j['uri']
        o_name = o_urn[o_urn.rfind('/') + 1:]
        o_source = 'Hbase'
      else:
        this.logger.info('*** Warning: "uri" is not found in %s' % j['name'])
        o_urn = ''
        o_name = ''
      if not j.has_key('fields'):
        o_fields = {"doc": None}
      else:
        o_fields = {}
        for f in j['fields']:
          o_field_name = f['name']
          o_fields[o_field_name] = dict(f)
        acp = AvroColumnParser(j, o_urn)
        o_field_list_ += acp.get_column_list_result()
      dataset_schema_record = DatasetSchemaRecord(o_name, json.dumps(j, sort_keys=True),
                                                  json.dumps(o_properties, sort_keys=True), json.dumps(o_fields), o_urn,
                                                  o_source, 'HBase', 'Table', None,None,None)
      schema_file_writer.append(dataset_schema_record)
      for fields in o_field_list_:
        field_record = DatasetFieldRecord(fields)
        field_file_writer.append(field_record)

    field_file_writer.close()
    schema_file_writer.close()
    input_json_file.close()

if __name__ == "__main__":
  args = sys.argv[1]
# parse the arguments and do transformation.
  t = HBaseTransform()
  t.transform(args[HBASE_LOCAL_RAW_META_DATA_KEY],args[HBASE_LOCAL_META_DATA_KEY],
              args[HBASE_LOCAL_FIELD_META_DATA_KEY])
