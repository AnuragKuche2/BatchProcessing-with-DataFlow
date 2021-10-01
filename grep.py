#!/usr/bin/env python

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery

class Split(beam.DoFn):
   def process(self, element):

      line_ = element.split(',')
      if len(line_) == 16:
         return [(line_[5], 1 )]

table_schema = {'fields': [
    {'name': 'Neighbourhood', 'type': 'STRING', 'mode': 'REQUIRED'},
   {'name': 'Count', 'type': 'INTEGER', 'mode': 'REQUIRED'}
]}

if __name__ == '__main__':
   options = PipelineOptions()
   p = beam.Pipeline(options=options)
   input = 'AB_NYC_2019.csv'
   output_prefix = 'test_op123'

   table_spec = bigquery.TableReference(
      projectId='yourprojectname',
      datasetId='yourdatasetid',
      tableId='yourtable')

   rows = (p |
           beam.io.ReadFromText(input, skip_header_lines=True) |
           beam.ParDo(Split()) | beam.combiners.Count.PerKey() | beam.io.WriteToText(output_prefix))

   bq = (
           rows
           | 'Writes To BigQuery' >> beam.io.WriteToBigQuery(
      table_spec,
      schema=table_schema,
      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
   ))

   p.run().wait_until_finish()
