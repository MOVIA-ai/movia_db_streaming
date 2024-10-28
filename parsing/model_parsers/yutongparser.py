from .vehicle_model_parser import VehicleModelParser

class YutongParser(VehicleModelParser):
    header = '11304'
    
    """YutongParser parser."""
    def transform(self, message):
        
        if 'can_8bit_value_1' in message:
            message['can_8bit_value_1'] = message['can_8bit_value_1'] * 0.4
        if 'can_32bit_value_1' in message:
            message['can_32bit_value_1'] = message['can_32bit_value_1'] / 200
    
        return message

