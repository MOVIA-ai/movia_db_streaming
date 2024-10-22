from vehicle_model_parser import VehicleModelParser

class JMCConquerParser(VehicleModelParser):
    """JMCConquerParser parser."""
    def transform_message(self, dict_input):
        
        if 'can_8bit_value_2' in dict_input:
            dict_input['can_8bit_value_2'] = dict_input['can_8bit_value_2'] / 2
    
        return dict_input
