#!/usr/bin/env python

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery

PROJECT = 'cloud-training-demos'
BUCKET = 'cloud-training-demos'

class Split(beam.DoFn):
   def process(self, element):

      line_ = element.split(',')
      if len(line_) == 16:
         return [(line_[5], 1 )]

table_schema = {'fields': [
    {'name': 'Neighbourhood', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'Count', 'type': 'INTEGER', 'mode': 'REQUIRED'}
]}

def run():
   argv = [
      '--project={0}'.format(PROJECT),
      '--job_name=examplejob2',
      '--save_main_session',
      '--staging_location=gs://{0}/staging/'.format(BUCKET),
      '--temp_location=gs://{0}/staging/'.format(BUCKET),
      '--region=us-central1',
      '--runner=DataflowRunner'
   ]

   options = PipelineOptions()
   p = beam.Pipeline(options=options)
   input = 'gs://{0}/javahelp/*.java'.format(BUCKET)
   output_prefix = 'gs://{0}/javahelp/output'.format(BUCKET)

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

   p.run()

if __name__ == '__main__':
   run()