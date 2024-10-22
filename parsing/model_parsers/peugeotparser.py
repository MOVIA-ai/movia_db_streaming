from vehicle_model_parser import VehicleModelParser

class PeugeotParser(VehicleModelParser):
    """PeugeotParser parser."""
    def transform_message(self, dict_input):
        
        if 'can_16bit_value_1' in dict_input:
            dict_input['can_16bit_value_1'] = dict_input['can_16bit_value_1'] / 100
    
        return dict_input
