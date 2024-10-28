from .vehicle_model_parser import VehicleModelParser

class PeugeotEPartnerParser(VehicleModelParser):
    header = '8722'

    """PeugeotEPartnerParser parser."""
    def transform(self, message):
        
        if 'can_16bit_value_1' in message:
            message['can_16bit_value_1'] = message['can_16bit_value_1'] / 100
    
        return message
