from vehicle_model_parser import VehicleModelParser, SHIFT_2_31


class BYDE5Parser(VehicleModelParser):
    header = ''
    
    """BYDE5Parser parser."""
    def transform_message(self, dict_input):
        
        if 'can_32bit_value_1' in dict_input:
            dict_input['can_32bit_value_1'] = int(dict_input['can_32bit_value_1']) / 10
        if 'can_16bit_value_1' in dict_input:
            # dict_input['can_16bit_value_1'] = int(hex(dict_input['can_16bit_value_1']).split('x')[1][-3:], 16) / 16
            aux_speed_check = (int(dict_input['can_16bit_value_1'] )-28672)/16
            dict_input['can_16bit_value_1'] = \
                int('0x'+hex(dict_input['can_16bit_value_1']).split('x')[1][-3:],16)/16
        #energia cargada
        if 'user_data_value_4' in dict_input and 'user_data_value_5' in dict_input:
            dict_input['e_cargada'] = (dict_input['user_data_value_5'] * SHIFT_2_31 + dict_input['user_data_value_4']) / 100000000
    
        #energia descargada    
        if 'user_data_value_2' in  dict_input.keys() and \
            'user_data_value_3' in  dict_input.keys() :
            
            dict_input['e_consumida'] = \
            (dict_input['user_data_value_3'] * SHIFT_2_31 + \
                dict_input['user_data_value_2'])/100000000

        #energía regenerada alt
        if 'can_32bit_value_2' in  dict_input.keys(): #Energía regenerada
            dict_input['can_32bit_value_2'] = int(dict_input['can_32bit_value_2'])/3600000 # W*s -> kWh

        #energía cargada alt
        if 'can_32bit_value_3' in  dict_input.keys(): #Energía cargada
            dict_input['can_32bit_value_3'] = int(dict_input['can_32bit_value_3'])/3600000 # W*s -> kWh

        #energia consumida
        if 'can_32bit_value_4' in  dict_input.keys(): #Energía consumida
            dict_input['can_32bit_value_4'] = int(dict_input['can_32bit_value_4'])/3600000 # W*s -> kWh

        return dict_input
    

