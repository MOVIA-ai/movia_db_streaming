from vehicle_model_parser import VehicleModelParser

class MGZSParser(VehicleModelParser):
    """MGZSParser parser."""
    def transform_message(self, dict_input):
        if 'can_16bit_value_1' in dict_input.keys():
            dict_input['can_16bit_value_1'] = dict_input['can_16bit_value_1']*0.1
        if 'can_16bit_value_2' in dict_input.keys():
            dict_input['can_16bit_value_2'] = dict_input['can_16bit_value_2']*0.01
        if 'can_16bit_value_3' in dict_input.keys():
            dict_input['can_16bit_value_3'] = dict_input['can_16bit_value_3']*0.01
        if 'can_32bit_value_2' in dict_input.keys():
            dict_input['can_32bit_value_2'] = dict_input['can_32bit_value_2']*0.1
        if 'can_32bit_value_3' in dict_input.keys():
            dict_input['can_32bit_value_3'] = dict_input['can_32bit_value_3']*0.1
        if 'can_32bit_value_4' in dict_input.keys():
            dict_input['can_32bit_value_4'] = dict_input['can_32bit_value_4']*0.1
        return dict_input
