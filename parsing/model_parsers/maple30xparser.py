from vehicle_model_parser import VehicleModelParser, SHIFT_2_31



class Maple30xParser(VehicleModelParser):
    """Maple30xParser parser."""
    def transform_message(self, dict_input):
        
        #odometro
        if 'can_16bit_value_5' in  dict_input.keys():
            dict_input['can_16bit_value_5'] = int(dict_input['can_16bit_value_5'])/10
        
        #soc
        if 'can_16bit_value_1' in dict_input.keys():
            dict_input['can_16bit_value_1'] = int(dict_input['can_16bit_value_1'])/640

        #energia cargada
        if 'user_data_value_4' in  dict_input.keys() and \
            'user_data_value_5' in  dict_input.keys() :
            
            dict_input['e_cargada'] = \
            (dict_input['user_data_value_5'] * SHIFT_2_31 + dict_input['user_data_value_4'])/100000000
        
        #energia descargada    
        if 'user_data_value_2' in  dict_input.keys() and \
            'user_data_value_3' in  dict_input.keys() :
            
            dict_input['e_consumida'] = \
            (dict_input['user_data_value_3'] * SHIFT_2_31 + \
                dict_input['user_data_value_2'])/100000000
    
        return dict_input
