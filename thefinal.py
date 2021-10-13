import logging
import argparse
import re
from datetime import datetime as dt
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam


PROJECT = ''
BUCKET = ''


def process(element):
    line_ = element.split(',')
    if len(line_) == 16:
        return [(line_[5], 1)]


def replace_nulls(element):
    return element.replace('NULL', '')


def parse_method(string_input):
    values = re.split(",", string_input)
    row = dict(
        zip(('Neighbourhood', 'Count'), values))
    return row


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input file to read. This can be a local file or '
             'a file in a Google Storage Bucket.',
        default=PROJECT)

    parser.add_argument('--output',
                        dest='output',
                        required=False,
                        help='Output BQ table to write results to.',
                        default='mydataset.airbnb2019')

    known_args, pipeline_args = parser.parse_known_args(argv)

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p
     | 'Read File' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
     | 'Replace Nulls' >> beam.Map(replace_nulls)
     | beam.Map(process)
     | beam.combiners.Count.PerKey()
     | beam.MapTuple(lambda icon, plant: '{}, {}'.format(icon, plant))
     | 'String To BigQuery Row' >> beam.Map(parse_method)

     | 'Write To BigQuery' >> beam.io.Write(
                beam.io.WriteToBigQuery(
                    known_args.output,
                    schema='Neighbourhood:STRING,Count:INTEGER',
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()