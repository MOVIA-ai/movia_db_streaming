import apache_beam as beam
import argparse
import logging
import ast
from parsing.gps_and_can_data_parsing import GPS_and_CAN_Data_Parsing


from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
#from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.options.pipeline_options import (PipelineOptions,
                                                  StandardOptions)
from apache_beam import typehints, trigger


from apache_beam.transforms.periodicsequence import PeriodicImpulse
from google.cloud import firestore
#import google.cloud.datastore as datastore

#from apache_beam.io.gcp.datastore.v1new.types import Entity
from helper_classes import CalculateOpsAlerts, WriteToFirestore, GetThresholdsDoFn, CalculateGeoAlerts #MyDictCoder #ThresholdUpdateFn

INPUT_SUBSCRIPTION = "projects/PROJECT_ID/subscriptions/SUBSCRIPTION_NAME"
BIGQUERY_TABLE = "PROJECT_ID:DATASET_NAME.TABLE_NAME"
BIGQUERY_SCHEMA = "timestamp:TIMESTAMP,attr1:FLOAT,msg:STRING"
ERROR_TABLE_SCHEMA = "message:STRING"

#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "key_antu_cloud_function.json"

IDENTIFIER_DICT = {'ident': 'device_hash', 'timestamp': 'event_timestamp',
'server_timestamp': 'created', 'bq_timestamp': 'updated'}

#DEFAULT DICT
SP_DICT_DEFAULT = {'external_powersource_voltage': '13', 'engine_ignition_status': '9'}

#JMC CONQUER
SP_DICT_JMC_CONQUER = {'can_8bit_value_2': '4', 'can_16bit_value_5': '21', 'can_8bit_value_1': '1'}
#MAXUS EV 30
SP_DICT_MAXUS_EV30 = {'can_8bit_value_1': '4', 'can_16bit_value_5': '21', 'can_16bit_value_6': '1', 'brake_acceleration':'26', 'turn_acceleration':'27', 'plugin_model_SOH': '15'}

#TO-DO Maxus_H6490
SP_DICT_MAXUS_H6490 = {'can_8bit_value_2': '4', 'vehicle_mileage':'20', 'brake_acceleration':'26', 'turn_acceleration':'27'}

#BYD e5
SP_DICT_BYD_E5 = {'can_8bit_value_1': '4', 'can_32bit_value_1': '21', 'can_16bit_value_1': '1', 'absolute_acceleration': '25', 'brake_acceleration':'26', 'turn_acceleration':'27', 
                'can_16bit_value_5': '22', 'e_cargada': '10', 'e_consumida': '11','can_32bit_value_2': '30','can_32bit_value_3': '31', 'can_32bit_value_4': '32', 'can_8bit_value_2': '15',
                'user_data_value_7': '35', 'user_data_value_8': '36'}

#HYUNDAI IONIQ
SP_DICT_HUYN_IONIQ = {'can_16bit_value_1': '4', 'can_16bit_value_2': '15', 'can_32bit_value_1': '21', 'can_16bit_value_3': '1', 'can_32bit_value_2': '30','can_32bit_value_3': '31', 'can_32bit_value_4': '32', 'user_data_value_3': '35', 'can_8bit_value_3': '36', 'absolute_acceleration': '25', 'brake_acceleration':'26', 'turn_acceleration':'27'}

#MGZS EV
SP_DICT_MGZS = {'can_16bit_value_1': '4', 'can_16bit_value_2': '15', 'can_32bit_value_1': '21', 'can_16bit_value_3': '1', 'can_32bit_value_2': '30','can_32bit_value_3': '31', 'can_32bit_value_4': '32', 'user_data_value_3': '35', 'can_8bit_value_3': '36', 'absolute_acceleration': '25', 'brake_acceleration':'26', 'turn_acceleration':'27'}


#raw fmb965 (piloto incognito Chilexpress)
SP_DICT_FMB965 = {'battery_level': '4'}

#Peugeot e-Partner
SP_DICT_PEUG_EPART = {'can_8bit_value_1': '4', 'user_data_value_1': '21', 'can_16bit_value_1': '1', 'brake_acceleration':'26', 'turn_acceleration':'27', 'absolute_acceleration': '25'}

#YUTONG E8
SP_DICT_YUTONG_E8 = {'can_8bit_value_1': '4', 'can_32bit_value_1': '21', 'can_8bit_value_2': '1', 'brake_acceleration':'26', 'turn_acceleration':'27', 'absolute_acceleration': '25'}

#MAPLE 30x
SP_DICT_MAPLE_30X = {'can_16bit_value_1': '4', 'can_16bit_value_5': '21',
'position_speed': '1', 'brake_acceleration':'26', 'turn_acceleration':'27',
'can_8bit_value_1': '22'}

#JAC N55
SP_DICT_JAC_N55 = {'can_8bit_value_1': '4', 'can_32bit_value_1': '21', 'can_8bit_value_2': '1', 'can_8bit_value_3':'15', 'brake_acceleration':'26', 'turn_acceleration':'27', 
                   'can_16bit_value_1': '18', 'can_16bit_value_2': '19'}

#JAC N55_alt
SP_DICT_JAC_N55_ALT = {'can_8bit_value_1': '4', 'can_8bit_value_2': '15', 'can_8bit_value_3': '1','can_32bit_value_1': '21','can_32bit_value_2': '30','can_32bit_value_3': '31', 
                        'can_32bit_value_4': '32','user_data_value_1': '19', 'user_data_value_2': '18', 'user_data_value_3': '35'}


#INTERNAL COMBUSTION ENGINE VEHICLE (w/Teltonika Device) TODO: replace odo by 20? 
SP_DICT_ICE_VEHICLE = {'vehicle_mileage':'20', 'can_vehicle_speed': '1', 'position_speed': '2', 'can_fuel_consumption':'5', 'can_fuel_level': '4',
    'can_engine_rpm': '41', 'can_engine_load_level': '42', 'can_throttle_pedal_level':'43'}

GPS_RENAME_DICT = {'position_latitude': 'latitude', 'position_longitude': 'longitude', 'position_altitude': 'altitude', 'position_speed': 'speed',
    'position_direction': 'direction', 'position_hdop': 'hdop', 'position_satellites':'satellites', 'vehicle_mileage':'vehicle_mileage'}

#TODO add common dictionary for flespi input

class CustomFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    bold_gray = "\x1b[38;1m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: bold_gray + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

_LOGGER = logging.getLogger(__name__)



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

    # Parsing arguments
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
               GPS_and_CAN_Data_Parsing(
                 schema_sp           = declared_args.sp_schema, 
                 schema_gps          = declared_args.gps_schema, 
                 runner              = runner,
                 sp_default_dict     = SP_DICT_DEFAULT,
                 gps_input_dict      = GPS_RENAME_DICT,
                 jmc_conquer_dict    = SP_DICT_JMC_CONQUER,
                 maxus_ev30_dict     = SP_DICT_MAXUS_EV30,
                 byd_e5_dict         = SP_DICT_BYD_E5,
                 fmb956_dic          = SP_DICT_FMB965,
                 hyundai_ioniq_dict  = SP_DICT_HUYN_IONIQ,
                 mgzs_dict           = SP_DICT_MGZS,
                 maxus_h6490_dict    = SP_DICT_MAXUS_H6490,
                 peugeot_dict        = SP_DICT_PEUG_EPART,
                 yutong_dict         = SP_DICT_YUTONG_E8,
                 maple_30x_dict      = SP_DICT_MAPLE_30X,
                 jac_n55_dict        = SP_DICT_JAC_N55,
                 jac_n55_alt_dict    = SP_DICT_JAC_N55_ALT,
                 ICE_vehicle_dict    = SP_DICT_ICE_VEHICLE,
                 identifier_dict     = IDENTIFIER_DICT)).with_outputs(
                   GPS_and_CAN_Data_Parsing.OUTPUT_GPS_TAG,
                   GPS_and_CAN_Data_Parsing.OUTPUT_SP_TAG,
                   GPS_and_CAN_Data_Parsing.OUTPUT_FS_TAG))
        
        sp_rows = parse_output[GPS_and_CAN_Data_Parsing.OUTPUT_SP_TAG]
        gps_rows = parse_output[GPS_and_CAN_Data_Parsing.OUTPUT_GPS_TAG]
        fs_rows = parse_output[GPS_and_CAN_Data_Parsing.OUTPUT_FS_TAG]

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

        writing_report_fs = \
            (fs_rows | "WritePositionsToFirestore" >> beam.ParDo(WriteToFirestore()))



# beam.window.FixedWindows(60),
#                                                                     trigger=trigger.AfterWatermark(
#                                                                         early=trigger.AfterProcessingTime(delay=60),
#                                                                         late=trigger.AfterCount(2)
#                                                                         ),
#                                                                     accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING,
#                                                                     allowed_lateness=60*60*24*7
#

        # alerts_node = (fs_rows | "ApplyWindow"   >> beam.WindowInto(beam.window.FixedWindows(120), #beam.window.GlobalWindows(),
        #                                                                 trigger=trigger.AfterWatermark(),
        #                                                                      # trigger=trigger.AfterAny(
        #                                                                      #            trigger.AfterProcessingTime(300),
        #                                                                      #            trigger.AfterCount(30)),
        #                                                                      accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
        #                                                                      allowed_lateness=60*60*24*7
        #                                                             )
        #                        #| "GroupByKey"    >> beam.GroupByKey()
        #                        | "GetThesholds"  >> beam.ParDo(GetThresholdsDoFn()).with_outputs(
        #                                                                             'threshold_enriched_data',
        #                                                                             'geofence_enriched_data'))

        # #tuple_type_hint = typehints.Tuple[float, float, float, float, float, int]

        # Ops_Alerts = \
        #     (alerts_node['threshold_enriched_data'] | "Calculate_Ops_Alerts" >> beam.ParDo(CalculateOpsAlerts())) #.with_input_types(tuple_type_hint)             
        #                                             # | "WriteToBigQuery_Alrts" >> WriteToBigQuery(
        #                                             #     'vivid-vent-265202:alerts_management.alerts',
        #                                             #     schema="alertId:STRING,vehicleId:STRING,typeId:STRING,severity:STRING,valorGenerado:FLOAT,valorUmbral:FLOAT,timestamp:TIMESTAMP",
        #                                             #     insert_retry_strategy = 'RETRY_NEVER',
        #                                             #     write_disposition=BigQueryDisposition.WRITE_APPEND,
        #                                             #     batch_size = 1))

        # geofence_alerts = \
        #     (alerts_node['geofence_enriched_data'] #| "ReWindowIntoGlobal" >> beam.WindowInto(beam.window.GlobalWindows(),
        #                                                                      # trigger=beam.trigger.Repeatedly(
        #                                                                      #     trigger.AfterAny(
        #                                                                      #         trigger.AfterProcessingTime(10),
        #                                                                      #         trigger.AfterCount(10))),
        #                                                                      # accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
        #                                                                      # allowed_lateness=0
        #                                             #                         )
        #                                            | "Calculate_Geo_Alerts" >> beam.ParDo(CalculateGeoAlerts()))


        ins_errors_sp = writing_report_sp['FailedRowsWithErrors'] 
        ins_errors_gps = writing_report_gps['FailedRowsWithErrors'] 

        _ = ((ins_errors_sp, ins_errors_gps) 
            | 'BQ Insertion Errors' >> beam.Flatten()
            | "JsonToString" >> beam.Map(lambda x: {"message": str(x)})
            | "WriteInsertErrorsToBQ" >> WriteToBigQuery(
                declared_args.output_table_errors, schema = ERROR_TABLE_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition='CREATE_IF_NEEDED', 
                insert_retry_strategy = 'RETRY_NEVER'))


        #beam.coders.registry.register_coder(typing.Dict, MyDictCoder)

        # alrt_subscription = 'projects/vivid-vent-265202/subscriptions/alerts_dev_sub_test'

        # config = (p | "Read PubSub topic" >> beam.io.gcp.pubsub.ReadFromPubSub(subscription=alrt_subscription)
        #             | beam.FlatMap(extractData)
        #             | "Update thresholds" >> beam.ParDo(ThresholdUpdateFn()))


if __name__ == "__main__":
    run()
