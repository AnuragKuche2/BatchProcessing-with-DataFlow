
import argparse
import csv
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery


class Split(beam.DoFn):
    def process(self, element):
        line_ = element.split(',')
        if len(line_) == 16:
            return [(line_[5], 1)]


PROJECT = ''
BUCKET = ''

table_schema = {'fields': [
    {'name': 'Neighbourhood', 'type': 'STRING'},
   {'name': 'Count', 'type': 'INTEGER'}
]}


def run():
    argv = [
        '--project={0}'.format(PROJECT),
        '--job_name=dataflowjobbq',
        '--save_main_session',
        '--staging_location=gs://{0}/staging/'.format(BUCKET),
        '--temp_location=gs://{0}/staging/'.format(BUCKET),
        '--region=us-central1',
        '--runner=DataflowRunner']


    table_spec = bigquery.TableReference(
      projectId=''.format(PROJECT),
      datasetId='{}:datasetid'.format(PROJECT),
      tableId="{}:datasetid.tablename".format(PROJECT))

    p = beam.Pipeline(argv=argv)
    input_file = 'gs://{0}/AB_NYC_2019.csv'.format(BUCKET)
    output_file = 'gs://{0}/output'.format(BUCKET)

    (p
     | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec)
     | beam.ParDo(Split())
     | beam.combiners.Count.PerKey()
     | beam.io.WriteToBigQuery(table_spec,
                               schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))


    p.run()


if __name__ == '__main__':
    run()