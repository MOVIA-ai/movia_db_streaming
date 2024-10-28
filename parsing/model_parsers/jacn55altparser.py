from .vehicle_model_parser import VehicleModelParser

class JACN55AltParser(VehicleModelParser):
    header = 'JAC_N55_alt'

    """JACN55AltParser parser."""
    def transform(self, message):

        #energia regenerada
        if 'can_32bit_value_2' in message.keys():
            message['can_32bit_value_2'] = int(message['can_32bit_value_2'])/3600000 # W*s -> kWh

        #energia cargada
        if 'can_32bit_value_3' in message.keys():
            message['can_32bit_value_3'] = int(message['can_32bit_value_3'])/3600000 # W*s -> kWh

        #energia consumida
        if 'can_32bit_value_4' in message.keys():
            message['can_32bit_value_4'] = int(message['can_32bit_value_4'])/3600000 # W*s -> kWh
    
        return message
