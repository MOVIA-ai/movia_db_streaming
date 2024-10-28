from common_utils.postgres_handler import PostgresHandler
import pandas as pd
import os
import importlib
import inspect
from parsing.model_parsers.vehicle_model_parser import VehicleModelParser
import logging


SP_DICT_DEFAULT = {'external_powersource_voltage': '13', 'engine_ignition_status': '9'}

GPS_RENAME_DICT = {'position_latitude': 'latitude', 'position_longitude': 'longitude', 'position_altitude': 'altitude', 'position_speed': 'speed',
    'position_direction': 'direction', 'position_hdop': 'hdop', 'position_satellites':'satellites', 'vehicle_mileage':'vehicle_mileage'}

IDENTIFIER_DICT = {'ident': 'device_hash', 'timestamp': 'event_timestamp',
'server_timestamp': 'created', 'bq_timestamp': 'updated'}


def fetch_vehicle_sensor_dict():
    query = '''SELECT vm.name, dsvm.can_bit_value, dsvm.device_sensor_fk
        FROM vehicle v
        JOIN vehicle_model vm ON v.vehicle_model_fk = vm.id
        JOIN asset a ON v.asset_fk = a.id
        JOIN device d ON v.device_fk = d.id
        LEFT JOIN inter_asset_device iad ON v.device_fk = iad.device_fk AND v.asset_fk = iad.asset_fk
        LEFT JOIN dict_sensor_vehicle_model dsvm ON vm.id = dsvm.vehicle_model_fk
        WHERE v.is_active = TRUE AND vm.is_active = TRUE AND a.is_active = TRUE AND d.is_active = TRUE;
        '''
    handler = PostgresHandler()
    df = handler.query_to_df(query)
    vehicle_dict = {}
    
    # Iterate through each row in the DataFrame
    for _, row in df.iterrows():
        vehicle_model_name = row['name']  # vehicle_model.name
        can_bit_value = row['can_bit_value']  # dict_sensor_vehicle_model.can_bit_value
        device_sensor_fk = row['device_sensor_fk']  # dict_sensor_vehicle_model.device_sensor_fk
        print(vehicle_model_name, can_bit_value, device_sensor_fk)
        
        # Check if the vehicle_model_name is already in the dictionary
        if vehicle_model_name not in vehicle_dict:
            vehicle_dict[vehicle_model_name] = {}
        
        # Add can_bit_value and device_sensor_fk to the inner dictionary
        vehicle_dict[vehicle_model_name][can_bit_value] = device_sensor_fk
    return vehicle_dict

def fetch_imei_dict():
    query = '''SELECT d.imei, iad.id
        from inter_asset_device iad
        JOIN device d ON iad.device_fk = d.id
        WHERE iad.active = TRUE AND d.is_active = TRUE;'''
    handler = PostgresHandler()
    df = handler.query_to_df(query)
    imei_dict = {}
    # Iterate through each row in the DataFrame
    for _, row in df.iterrows():
        imei = row['imei']
        iad_id = row['id']
        
        # Map imei to iad.id
        imei_dict[imei] = iad_id
    return imei_dict

def model_parsers_dict():
    # Directory where the Parser files are located
    parser_dir = 'parsing/model_parsers'
    parser_module_path = parser_dir.replace('/', '.')
    if not os.path.exists(parser_dir):
        raise FileNotFoundError(f"Directory '{parser_dir}' does not exist!")
    # Initialize the model_parsers dictionary
    model_parsers = {}
    # Loop through the files in the directory
    for filename in os.listdir(parser_dir):
        # Check for Python files, ignore __init__.py or non-Python files
        if filename.endswith('.py') and filename != '__init__.py':
            module_name = filename[:-3]  # Remove '.py'
            print(module_name, f'{parser_module_path}.{module_name}')
            module = importlib.import_module(f'{parser_module_path}.{module_name}')
            # Iterate over all attributes of the module
            for name, obj in inspect.getmembers(module):
                # Check if the object is a class and a subclass of VehicleModelParser
                if inspect.isclass(obj) and issubclass(obj, VehicleModelParser) and obj is not VehicleModelParser:
                    # Add the class to the dictionary using its `header` attribute
                    model_parsers[obj.header] = obj

    # Print to confirm dynamic imports
    print(f"Model parsers loaded: {list(model_parsers.keys())}")
    return model_parsers


def rename_and_melt(element, rename_dict, pivot=True, melt_ids={}):
    aux_df = pd.DataFrame([element])
    aux_df = aux_df.rename(columns=rename_dict)

    for key in rename_dict.keys():
        if rename_dict[key] not in aux_df.columns:
            aux_df[rename_dict[key]] = None

    aux_df['created'] = pd.to_datetime(aux_df['created'], unit='s')

    # uniformizar SOC a partes por mil.
    if '4' in aux_df.columns.tolist():
        aux_df['4'] = aux_df['4'] * 10 

    renamed = list(rename_dict.values()) #[rename_dict[str(name)] for name in rename_dict.keys()]

    if pivot:
        aux_df = aux_df[renamed].melt(
            id_vars=list(melt_ids.values()), 
            var_name='sensor_type_id', value_name='value')
        aux_df['sensor_type_id'] = [
            int(x) for x in aux_df['sensor_type_id']]
        return aux_df.to_dict(orient='records')

    else:
        aux_df = aux_df[renamed]
        return aux_df.to_dict(orient='records')[0]
    
def decode_schema_str(schema):
    """Decode schema string to a dictionary with key and type."""
    out_dict = {}
    fields = schema.split(",")
    for field in fields:
        key, key_type = field.split(":")
        out_dict[key] = key_type
    return out_dict


class CustomFormatter(logging.Formatter):

    gray = "\x1b[38;20m"
    bold_gray = "\x1b[38;1m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: gray + format + reset,
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