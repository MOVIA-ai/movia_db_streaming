import apache_beam as beam
import json
import traceback
from apache_beam.io.gcp.pubsub import PubsubMessage
from parsing.utils import fetch_vehicle_sensor_dict, fetch_imei_dict, model_parsers_dict, rename_and_melt, \
    decode_schema_str
from parsing.utils import _LOGGER

class GPSAndCANDataParsing(beam.DoFn):
    """ Custom ParallelDo class to apply a custom transformation for GPS and CAN data parsing """

    OUTPUT_GPS_TAG = "gps_rows"
    OUTPUT_SP_TAG = "sp_rows"
    OUTPUT_FS_TAG = "fs_rows"

    def __init__(self, schema_sp, schema_gps, runner, device_imei_field, sp_default_dict, gps_input_dict, 
                 identifier_dict, extras_renaming_dict):
        """Initialize the DoFn with necessary schemas, dictionaries, and runner information."""
        self.info = runner == "DirectRunner"
        self.schema_sp = schema_sp
        self.schema_gps = schema_gps
        self.device_imei_field = device_imei_field
        self.gps_input_dict = gps_input_dict
        self.sp_default_dict = sp_default_dict
        self.identifier_dict = identifier_dict
        self.extras_renaming_dict = extras_renaming_dict
        
        self.load_parsers()
        self.create_vehicle_sensor_dict()
        self.create_imei_dict()

    def create_vehicle_sensor_dict(self):
        """Fetch vehicle sensor dictionary for CAN data parsing."""
        self.sp_model_dict = fetch_vehicle_sensor_dict()
        _LOGGER.debug(self.sp_model_dict)

    def create_imei_dict(self):
        """Fetch IMEI dictionary (if needed elsewhere)."""
        self.imei_dict = fetch_imei_dict()
        _LOGGER.debug(self.imei_dict)

    def load_parsers(self):
        """Load model parsers for specific transformations."""
        self.model_parsers = model_parsers_dict()
        _LOGGER.debug(f'loaded {len(self.model_parsers)} model parsers')
        _LOGGER.debug(self.model_parsers)

    def to_runner_api_parameter(self, unused_context):
        """Return a URN and payload for the Beam runner API."""
        return "beam:transforms:custom_parsing:custom_v0", None

    def process(self, element: PubsubMessage, timestamp=beam.DoFn.TimestampParam, window=beam.DoFn.WindowParam):
        """Process a PubSub message, parse GPS and CAN data, and emit parsed rows."""
        from apache_beam import pvalue
        from datetime import datetime
        import pandas as pd

        try:
            # Parse the PubSub message
            dict_data = json.loads(element.data.decode("utf-8"))
            dict_attributes = element.attributes
            message = {**dict_data, **dict_attributes}
            message["bq_timestamp"] = datetime.now().timestamp()

            _LOGGER.debug(f'Raw message received from Pub/Sub: {message}')
            
            # Sanitize the message keys
            message = {key.replace(".", "_"): value for key, value in message.items()}
            message = {key: int(value) if isinstance(value, bool) else value for key, value in message.items()}

            # Replace 'imei' field with 'asset_device_fk' mapped value if present
            imei = message.get(self.device_imei_field)
            if imei and imei in self.imei_dict:
                message["asset_device_fk"] = self.imei_dict[imei]
                del message[self.device_imei_field]
            else:
                _LOGGER.warning(f'No imei field ({self.device_imei_field}), discarding. Full message: {message}')
                return

            if 'vehicle_model_id' in message:
                parser_class = self.model_parsers.get(message['vehicle_model_id'])
                if parser_class:
                    parser_instance = parser_class()
                    message = parser_instance.transform(message)
                sp_input_dict = self.sp_model_dict.get(message['vehicle_model_id'], {})
                sp_input_dict.update(self.sp_default_dict)
            else:
                _LOGGER.warning(f'No model in message, discarding. Full message: {message}')
                return
            
            print(sp_input_dict)
            # Handle extra keys
            extras_dict = {key: value for key, value in message.items() 
                         if key not in {**sp_input_dict, **self.gps_input_dict, **self.identifier_dict}}
            renamed_extras = {self.extras_renaming_dict.get(key, key): value 
                              for key, value in extras_dict.items()}

            key_error = bool(renamed_extras)

            if key_error:
                _LOGGER.debug(f"Some keys are not in specified schema: {extras_dict}")
                pass

            # Handle CAN data
            missing_sp_keys = [key for key in sp_input_dict if key not in message]
            sp_output_dict = {}
            discarded_sp_data = False

            if len(missing_sp_keys) < len(sp_input_dict):
                sp_output_dict = rename_and_melt(
                    message,
                    rename_dict={**sp_input_dict, **self.identifier_dict},
                    melt_ids=self.identifier_dict
                )
                yield pvalue.TaggedOutput(self.OUTPUT_SP_TAG, sp_output_dict)
                _LOGGER.debug(f"SP Output: {sp_output_dict}")
                
                #but if there are still missing keys (i.e, only partial CAN data)
                if missing_sp_keys:
                    _LOGGER.warning(f'CAN Keys {missing_sp_keys} not in message. Full message: {message}')
                    pass
            else:
                discarded_sp_data = True

            # Handle GPS data
            missing_gps_keys = [key for key in self.gps_input_dict if key not in message]

            if len(missing_gps_keys) < len(self.gps_input_dict):
                gps_output_dict = rename_and_melt(
                    message, 
                    rename_dict={**self.gps_input_dict, **self.identifier_dict}, 
                    pivot=False
                )
                gps_output_dict.update({key: None for key in decode_schema_str(self.schema_gps) if key not in gps_output_dict})
                gps_output_dict["extras"] = json.dumps(renamed_extras)

                _LOGGER.debug(f"GPS Output: {gps_output_dict}")
            
                yield pvalue.TaggedOutput(self.OUTPUT_GPS_TAG, gps_output_dict)
                
                #but if there are still missing keys (i.e, only partial CAN data)
                if missing_gps_keys:
                    #_LOGGER.warning(f'GPS Keys {missing_keys} not in message. Full message: {message}')
                    pass

            elif discarded_sp_data:
                some_data = False
                #_LOGGER.debug(f'No GPS nor CAN data, discarding: {message}')

            # Handle final state data for FS processing
            last_state = rename_and_melt(
                message, 
                rename_dict={**sp_input_dict, **self.identifier_dict, **self.gps_input_dict}, 
                pivot=False, melt_ids=self.identifier_dict
            )

            odometer_key = '21' if '21' in last_state else 'vehicle_mileage'
            realtime_tracking_keys = ['1', '4', '15', odometer_key, 'latitude', 'longitude', 'event_timestamp']
            values = {key: last_state[key] for key in realtime_tracking_keys if key in last_state}

            # _LOGGER.debug(f"Data to FS: {imei}: {values}")
            yield pvalue.TaggedOutput(self.OUTPUT_FS_TAG, (imei, values))

        except Exception as e:
            _LOGGER.warning(f'ERROR DECODING {element.data}')
            _LOGGER.error(traceback.format_exc())
            _LOGGER.error(traceback.format_stack())

