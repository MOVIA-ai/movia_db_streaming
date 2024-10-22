from vehicle_model_parser import VehicleModelParser

class JACN55AltParser(VehicleModelParser):
    """JACN55AltParser parser."""
    def transform_message(self, dict_input):

        #energia regenerada
        if 'can_32bit_value_2' in dict_input.keys():
            dict_input['can_32bit_value_2'] = int(dict_input['can_32bit_value_2'])/3600000 # W*s -> kWh

        #energia cargada
        if 'can_32bit_value_3' in dict_input.keys():
            dict_input['can_32bit_value_3'] = int(dict_input['can_32bit_value_3'])/3600000 # W*s -> kWh

        #energia consumida
        if 'can_32bit_value_4' in dict_input.keys():
            dict_input['can_32bit_value_4'] = int(dict_input['can_32bit_value_4'])/3600000 # W*s -> kWh
    
        return dict_input
