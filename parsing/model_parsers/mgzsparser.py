from .vehicle_model_parser import VehicleModelParser

class MGZSParser(VehicleModelParser):
    header = '7930'

    """MGZSParser parser."""
    def transform(self, message):
        if 'can_16bit_value_1' in message.keys():
            message['can_16bit_value_1'] = message['can_16bit_value_1']*0.1
        if 'can_16bit_value_2' in message.keys():
            message['can_16bit_value_2'] = message['can_16bit_value_2']*0.01
        if 'can_16bit_value_3' in message.keys():
            message['can_16bit_value_3'] = message['can_16bit_value_3']*0.01
        if 'can_32bit_value_2' in message.keys():
            message['can_32bit_value_2'] = message['can_32bit_value_2']*0.1
        if 'can_32bit_value_3' in message.keys():
            message['can_32bit_value_3'] = message['can_32bit_value_3']*0.1
        if 'can_32bit_value_4' in message.keys():
            message['can_32bit_value_4'] = message['can_32bit_value_4']*0.1
        return message
