import apache_beam as beam
import argparse
import logging
import ast
import os
from parsing.gps_and_can_data_parsing import GPSAndCANDataParsing
from parsing.utils import _LOGGER, CustomFormatter
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import (PipelineOptions,
                                                  StandardOptions)
from helper_classes import WriteToFirestore
from dotenv import load_dotenv

INPUT_SUBSCRIPTION = "projects/PROJECT_ID/subscriptions/SUBSCRIPTION_NAME"
BIGQUERY_SCHEMA = "timestamp:TIMESTAMP,attr1:FLOAT,msg:STRING"
ERROR_TABLE_SCHEMA = "message:STRING"

IDENTIFIER_DICT = {'ident': 'device_hash', 'timestamp': 'event_timestamp',
'server_timestamp': 'created', 'bq_timestamp': 'updated'}

#DEFAULT DICT
SP_DICT_DEFAULT = {'external_powersource_voltage': '13', 'engine_ignition_status': '9'}

GPS_RENAME_DICT = {'position_latitude': 'latitude', 'position_longitude': 'longitude', 'position_altitude': 'altitude', 'position_speed': 'speed',
    'position_direction': 'direction', 'position_hdop': 'hdop', 'position_satellites':'satellites', 'vehicle_mileage':'vehicle_mileage'}

# Helper function to load and set credentials
def set_google_application_credentials(credentials_file):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_file



def extractData(x):
  try:

    decoded_message = x.decode("utf-8")
    message_dict = ast.literal_eval(decoded_message)

    device_hash     = message_dict['device_hash']
    threshold_type  = message_dict['threshold_type']
    threshold_value = message_dict['threshold_value']
  
    yield (device_hash, [threshold_type, threshold_value])

  except Exception as e:
    # In case of an error, don't yield anything
    logging.error(f"Error processing element {x}: {e}")


def run():
    load_dotenv()
    # bq_credentials = os.environ.get('GCL_NEW_CREDENTIALS_PATH')
    # bq_credentials = os.environ.get('GCL_CREDENTIALS_PATH')
    # set_google_application_credentials(bq_credentials)
    # Parsing arguments
    pubsub_credentials = os.environ.get('GCL_NEW_CREDENTIALS_PATH')
    set_google_application_credentials(pubsub_credentials)
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_subscription",
        help='Input PubSub subs: "projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
        default=INPUT_SUBSCRIPTION,
    )
    parser.add_argument(
        "--output_table_sp", help="Output BigQuery Table for Sensor Data",)
    parser.add_argument(
        "--output_table_gps", help="Output BigQuery Table for GPS Data")
    parser.add_argument(
        "--output_table_errors", help="Output BigQuery Table for Errors")
    parser.add_argument(
        "--gps_schema", help="Output BigQuery Schema in text format for gps table",
        default=BIGQUERY_SCHEMA)
    parser.add_argument("--sp_schema", default = BIGQUERY_SCHEMA)
    parser.add_argument('--local_logging', action='store_true', 
        help='if set, log at DEBUG level locally')

    declared_args, pipeline_args = parser.parse_known_args()

    if declared_args.local_logging:
        _LOGGER.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(CustomFormatter())
        _LOGGER.addHandler(ch)
        _LOGGER.propagate = False

    # Creating pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    runner = pipeline_options.get_all_options()["runner"]
  
    # Defining our pipeline and its steps    
    with beam.Pipeline(options=pipeline_options) as p:
        

        parse_output = \
            (p | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription=declared_args.input_subscription, 
                timestamp_attribute=None, with_attributes=True)
             | "Data Parsing" >> beam.ParDo(
               GPSAndCANDataParsing(
                 schema_sp           = declared_args.sp_schema, 
                 schema_gps          = declared_args.gps_schema, 
                 runner              = runner,
                 sp_default_dict     = SP_DICT_DEFAULT,
                 gps_input_dict      = GPS_RENAME_DICT,
                 identifier_dict     = IDENTIFIER_DICT)).with_outputs(
                   GPSAndCANDataParsing.OUTPUT_GPS_TAG,
                   GPSAndCANDataParsing.OUTPUT_SP_TAG,
                   GPSAndCANDataParsing.OUTPUT_FS_TAG))
        
        sp_rows = parse_output[GPSAndCANDataParsing.OUTPUT_SP_TAG]
        gps_rows = parse_output[GPSAndCANDataParsing.OUTPUT_GPS_TAG]
        fs_rows = parse_output[GPSAndCANDataParsing.OUTPUT_FS_TAG]

        

        writing_report_sp = \
            (sp_rows | 'FLATMAP' >> beam.FlatMap(lambda elements: elements)
                     | "WriteToBigQuery_SP" >> WriteToBigQuery(
                declared_args.output_table_sp,
                schema=declared_args.sp_schema,
                insert_retry_strategy = 'RETRY_NEVER',
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                batch_size = 20))

        writing_report_gps = \
            (gps_rows | "WriteToBigQuery_GPS" >> WriteToBigQuery(
                declared_args.output_table_gps,
                schema=declared_args.gps_schema,
                insert_retry_strategy = 'RETRY_NEVER',
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                batch_size = 20))

        # writing_report_fs = \
        #     (fs_rows | "WritePositionsToFirestore" >> beam.ParDo(WriteToFirestore()))


        # ins_errors_sp = writing_report_sp['FailedRowsWithErrors'] 
        # ins_errors_gps = writing_report_gps['FailedRowsWithErrors'] 

        # _ = ((ins_errors_sp, ins_errors_gps) 
        #     | 'BQ Insertion Errors' >> beam.Flatten()
        #     | "JsonToString" >> beam.Map(lambda x: {"message": str(x)})
        #     | "WriteInsertErrorsToBQ" >> WriteToBigQuery(
        #         declared_args.output_table_errors, schema = ERROR_TABLE_SCHEMA,
        #         write_disposition=BigQueryDisposition.WRITE_APPEND,
        #         create_disposition='CREATE_IF_NEEDED', 
        #         insert_retry_strategy = 'RETRY_NEVER'))


if __name__ == "__main__":
    run()
