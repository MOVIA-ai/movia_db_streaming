SHIFT_2_31         = 2147483648 # 2**31


class VehicleModelParser:
    """Parent class for parsing vehicle data."""

    def __init__(self, sp_input_dict):
        self.sp_input_dict = sp_input_dict

    def transform_message(self, dict_input):
        """Base method for transforming data, to be overridden by subclasses."""
        raise NotImplementedError("This method should be implemented by the subclass.")

    def base_transformation(self, dict_input):
        """Common transformation logic across all models."""
        # Perform any transformations that are common across all vehicle models
        if 'common_key' in dict_input:
            dict_input['common_key'] = dict_input['common_key'] / 100
        return dict_input
    