from .vehicle_model_parser import VehicleModelParser

class JMCConquerParser(VehicleModelParser):
    header = '5030'
    
    """JMCConquerParser parser."""
    def transform(self, message):
        
        if 'can_8bit_value_2' in message:
            message['can_8bit_value_2'] = message['can_8bit_value_2'] / 2
    
        return message
