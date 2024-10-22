from vehicle_model_parser import VehicleModelParser

class YutongParser(VehicleModelParser):
    """YutongParser parser."""
    def transform_message(self, dict_input):
        
        if 'can_8bit_value_1' in dict_input:
            dict_input['can_8bit_value_1'] = dict_input['can_8bit_value_1'] * 0.4
        if 'can_32bit_value_1' in dict_input:
            dict_input['can_32bit_value_1'] = dict_input['can_32bit_value_1'] / 200
    
        return dict_input

