from .vehicle_model_parser import VehicleModelParser, SHIFT_2_31


class BYDE5Parser(VehicleModelParser):
    header = '886'
    
    """BYDE5Parser parser."""
    def transform(self, message):
        
        if 'can_32bit_value_1' in message:
            message['can_32bit_value_1'] = int(message['can_32bit_value_1']) / 10
        if 'can_16bit_value_1' in message:
            # message['can_16bit_value_1'] = int(hex(message['can_16bit_value_1']).split('x')[1][-3:], 16) / 16
            aux_speed_check = (int(message['can_16bit_value_1'] )-28672)/16
            message['can_16bit_value_1'] = \
                int('0x'+hex(message['can_16bit_value_1']).split('x')[1][-3:],16)/16
        #energia cargada
        if 'user_data_value_4' in message and 'user_data_value_5' in message:
            message['e_cargada'] = (message['user_data_value_5'] * SHIFT_2_31 + message['user_data_value_4']) / 100000000
    
        #energia descargada    
        if 'user_data_value_2' in  message.keys() and \
            'user_data_value_3' in  message.keys() :
            
            message['e_consumida'] = \
            (message['user_data_value_3'] * SHIFT_2_31 + \
                message['user_data_value_2'])/100000000

        #energía regenerada alt
        if 'can_32bit_value_2' in  message.keys(): #Energía regenerada
            message['can_32bit_value_2'] = int(message['can_32bit_value_2'])/3600000 # W*s -> kWh

        #energía cargada alt
        if 'can_32bit_value_3' in  message.keys(): #Energía cargada
            message['can_32bit_value_3'] = int(message['can_32bit_value_3'])/3600000 # W*s -> kWh

        #energia consumida
        if 'can_32bit_value_4' in  message.keys(): #Energía consumida
            message['can_32bit_value_4'] = int(message['can_32bit_value_4'])/3600000 # W*s -> kWh

        return message
    

