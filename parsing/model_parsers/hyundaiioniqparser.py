from vehicle_model_parser import VehicleModelParser

class HyundaiIoniqParser(VehicleModelParser):
    """HyundaiIoniqParser parser."""
    def transform_message(self, dict_input):
        
        for key in ['can_16bit_value_1', 'can_16bit_value_2', 'can_16bit_value_3', 'can_32bit_value_3', 'can_32bit_value_4']:
            if key in dict_input:
                dict_input[key] = dict_input[key] * 0.1
    
        return dict_input
