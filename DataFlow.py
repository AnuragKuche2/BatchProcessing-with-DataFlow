import argparse
import csv
import logging
import os

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions




class Split(beam.DoFn):
    def process(self, element):
        line_ = element.split(',')
        if len(line_) == 16:
            return [(line_[5], 1 )]

def addKey(row):
    return (1, row)

def sortGroupedData(row):
    (keyNumber, sortData) = row
    sortData.sort(key=lambda x: x[1], reverse=True)
    return sortData

PROJECT=''
BUCKET=''

def run():
    argv = [
    '--project={0}'.format(PROJECT),
    '--job_name=examplejob2',
    '--save_main_session',
    '--staging_location=gs://{0}/staging/'.format(BUCKET),
    '--temp_location=gs://{0}/staging/'.format(BUCKET),
    '--region=us-central1',
    '--runner=DataflowRunner']

    p = beam.Pipeline(argv=argv)
    input_file = 'gs://$BUCKET/AB_NYC_2019.csv'
    output_file = 'gs://$BUCKET/output'

    (p
    | beam.io.ReadFromText(input_file, skip_header_lines=True)
    | beam.ParDo(Split())
    | beam.combiners.Count.PerKey()
    | 'AddKey' >> beam.Map(addKey)
    | 'GroupByKey' >> beam.GroupByKey()
    | 'SortGroupedData' >> beam.Map(sortGroupedData)
    | beam.io.WriteToText(output_file))

    p.run()

if __name__ == '__main__':
    run()