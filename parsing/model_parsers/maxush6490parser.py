from vehicle_model_parser import VehicleModelParser

class MaxusH6490Parser(VehicleModelParser):
    """MaxusH6490Parser parser."""
    def transform_message(self, dict_input):
        
        if 'can_8bit_value_2' in dict_input:
            dict_input['can_8bit_value_2'] = dict_input['can_8bit_value_2'] * 0.4
    
        return dict_input
