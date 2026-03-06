from typing import Any, Dict

class ConfigureFromDict:
    """A mixin for configuring Dagster configurable resources from a dictionary.
    
    This provides a consistent interface across different custom components 
    to instantiate themselves from a parsed configuration dict (e.g. from YAML/JSON).
    """

    @classmethod
    def configure(cls, config: Dict[str, Any]) -> "ConfigureFromDict":
        """Instantiate the resource from a configuration dictionary.

        Args:
            config: A dictionary representing the configuration for this resource.

        Returns:
            An instance of the class implementing this mixin.
        """
        raise NotImplementedError("Subclasses must implement the 'configure' method.")
