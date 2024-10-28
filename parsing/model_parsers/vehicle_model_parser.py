SHIFT_2_31         = 2147483648 # 2**31


class VehicleModelParser:
    header = ''
    """Parent class for parsing vehicle data."""

    def transform(self, message):
        """Base method for transforming data, to be overridden by subclasses."""
        raise NotImplementedError("This method should be implemented by the subclass.")

    def base_transformation(self, message):
        """Common transformation logic across all models."""
        # Perform any transformations that are common across all vehicle models
        if 'common_key' in message:
            message['common_key'] = message['common_key'] / 100
        return message
    