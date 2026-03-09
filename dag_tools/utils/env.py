import copy
import os
from typing import Any, Dict, Optional
from dagster import EnvVar


class ExpandedEnvVar(EnvVar):
    """Class used to represent an environment variable in the Dagster config system including prefix and postfix.

    This class is intended to be used to populate config fields or resources.
    The environment variable will be resolved to a string value when the config is loaded.

    To access the value of the environment variable, use the `get_value` method.
    """

    def __new__(cls, value: str, prefix: str = "", postfix: str = ""):
        instance = EnvVar.__new__(cls, value)
        instance.prefix = prefix
        instance.postfix = postfix
        return instance

    def get_value(self, default: Optional[str] = None) -> Optional[str]:
        """Returns the value of the environment variable, or the default value if the
        environment variable is not set. If no default is provided, None will be returned.
        """
        value = os.getenv(self.env_var_name, default=default)
        if value is None:
            return None
        return f"{self.prefix}{value}{self.postfix}"


def update_from_env(config: Dict[str, Any], eval: bool = False) -> Dict[str, Any]:
    """Recursively checks a dictionary for 'env' keys and translates them into Dagster EnvVar objects.
    
    If eval is True, the environment variables are instantly resolved to their system values.
    Returns a deepcopy of the configuration, ensuring no unexpected state mutation occurs.
    """
    config_copy = copy.deepcopy(config)
    
    for key, value in config_copy.items():
        if isinstance(value, dict):
            if "env" in value:
                if "postfix" in value or "prefix" in value:
                    config_copy[key] = ExpandedEnvVar(
                        value["env"],
                        postfix=value.get("postfix", ""),
                        prefix=value.get("prefix", "")
                    )
                else:
                    config_copy[key] = EnvVar(value["env"])
                    
                if eval:
                    config_copy[key] = config_copy[key].get_value()
            else:
                config_copy[key] = update_from_env(value, eval)
                
        elif eval and isinstance(value, EnvVar):
            config_copy[key] = value.get_value()
            
    return config_copy
