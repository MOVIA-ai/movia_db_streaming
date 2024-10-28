from .vehicle_model_parser import VehicleModelParser, SHIFT_2_31



class Maple30xParser(VehicleModelParser):
    header = '6285'
    
    """Maple30xParser parser."""
    def transform(self, message):
        
        #odometro
        if 'can_16bit_value_5' in  message.keys():
            message['can_16bit_value_5'] = int(message['can_16bit_value_5'])/10
        
        #soc
        if 'can_16bit_value_1' in message.keys():
            message['can_16bit_value_1'] = int(message['can_16bit_value_1'])/640

        #energia cargada
        if 'user_data_value_4' in  message.keys() and \
            'user_data_value_5' in  message.keys() :
            
            message['e_cargada'] = \
            (message['user_data_value_5'] * SHIFT_2_31 + message['user_data_value_4'])/100000000
        
        #energia descargada    
        if 'user_data_value_2' in  message.keys() and \
            'user_data_value_3' in  message.keys() :
            
            message['e_consumida'] = \
            (message['user_data_value_3'] * SHIFT_2_31 + \
                message['user_data_value_2'])/100000000
    
        return message
