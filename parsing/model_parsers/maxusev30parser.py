from .vehicle_model_parser import VehicleModelParser

class MaxusEV30Parser(VehicleModelParser):
    header = '6405'
    
    """MaxusEV30Parser parser."""
    def transform(self, message):
        
        if 'can_8bit_value_2' in message:
            message['can_8bit_value_2'] = message['can_8bit_value_2'] * 0.4
    
        return message
