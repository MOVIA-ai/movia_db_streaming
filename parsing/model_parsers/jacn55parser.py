from .vehicle_model_parser import VehicleModelParser

class JACN55Parser(VehicleModelParser):
    header = '4595'

    """JACN55Parser parser."""
    def transform(self, message):
        
        #soc
        if 'can_8bit_value_1' in message.keys():
            message['can_8bit_value_1'] = int(message['can_8bit_value_1'])/2

        #odometer
        if 'can_32bit_value_1' in message.keys():
            message['can_32bit_value_1'] = int(hex(message['can_32bit_value_1']).split('x')[1][:-2],16)/10

        #soh
        if 'can_8bit_value_3' in message.keys():
            message['can_8bit_value_3'] = int(message['can_8bit_value_3'])/2

        #corriente bateria
        if 'can_16bit_value_1' in message.keys():
            message['can_16bit_value_1'] = int(message['can_16bit_value_1'])/10-300.0

        #voltaje bateria
        if 'can_16bit_value_2' in message.keys():
            message['can_16bit_value_2'] = int(message['can_16bit_value_2'])/10
    
        return message
