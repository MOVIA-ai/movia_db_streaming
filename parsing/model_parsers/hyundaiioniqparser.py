from .vehicle_model_parser import VehicleModelParser

class HyundaiIoniqParser(VehicleModelParser):
    header = '3775'

    """HyundaiIoniqParser parser."""
    def transform(self, message):
        
        for key in ['can_16bit_value_1', 'can_16bit_value_2', 'can_16bit_value_3', 'can_32bit_value_3', 'can_32bit_value_4']:
            if key in message:
                message[key] = message[key] * 0.1
    
        return message
