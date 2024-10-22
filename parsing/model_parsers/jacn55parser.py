from vehicle_model_parser import VehicleModelParser

class JACN55Parser(VehicleModelParser):
    """JACN55Parser parser."""
    def transform_message(self, dict_input):
        
        #soc
        if 'can_8bit_value_1' in dict_input.keys():
            dict_input['can_8bit_value_1'] = int(dict_input['can_8bit_value_1'])/2

        #odometer
        if 'can_32bit_value_1' in dict_input.keys():
            dict_input['can_32bit_value_1'] = int(hex(dict_input['can_32bit_value_1']).split('x')[1][:-2],16)/10

        #soh
        if 'can_8bit_value_3' in dict_input.keys():
            dict_input['can_8bit_value_3'] = int(dict_input['can_8bit_value_3'])/2

        #corriente bateria
        if 'can_16bit_value_1' in dict_input.keys():
            dict_input['can_16bit_value_1'] = int(dict_input['can_16bit_value_1'])/10-300.0

        #voltaje bateria
        if 'can_16bit_value_2' in dict_input.keys():
            dict_input['can_16bit_value_2'] = int(dict_input['can_16bit_value_2'])/10
    
        return dict_input
