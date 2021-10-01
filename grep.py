#!/usr/bin/env python

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.transforms.combiners import ToList


class Split(beam.DoFn):
   def process(self, element):
      line_ = element.split(',')
      if len(line_) == 16:
         return [(line_[5], 1 )]

table_schema = {'fields': [
    {'name': 'Neighbourhood', 'type': 'STRING'},
   {'name': 'Count', 'type': 'INTEGER'}
]}

def addKey(row):
    return (1, row)

def sortGroupedData(row):
    (keyNumber, sortData) = row
    sortData.sort(key=lambda x: x[1], reverse=True)
    return sortData


if __name__ == '__main__':
    options = PipelineOptions()
    p = beam.Pipeline(options=options)
    inputfile = 'gs://qwiklabs-gcp-02-0b434aff0c49/AB_NYC_2019.csv'
    output_prefix = 'gs://qwiklabs-gcp-02-0b434aff0c49/test_op11'


    table_spec = bigquery.TableReference(
      projectId='qwiklabs-gcp-02-0b434aff0c49',
      datasetId='qwiklabs-gcp-02-0b434aff0c49:datasetid',
      tableId= "qwiklabs-gcp-02-0b434aff0c49:datasetid.tablename")


    rows = (p |
            beam.io.ReadFromText(inputfile, skip_header_lines=True) |
            beam.ParDo(Split()) | beam.combiners.Count.PerKey()
            | 'AddKey' >> beam.Map(addKey)
            | 'GroupByKey' >> beam.GroupByKey()
            | 'SortGroupedData' >> beam.Map(sortGroupedData)
            | beam.io.WriteToText(output_prefix))

    bq = (
           rows
           | 'Writes To BigQuery' >> beam.io.WriteToBigQuery(
      table_spec,
      schema=table_schema,
      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
   ))

    p.run().wait_until_finish()
