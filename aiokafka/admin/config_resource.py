from enum import IntEnum


class ConfigResourceType(IntEnum):

    BROKER = 4
    TOPIC = 2


class ConfigResource:

    def __init__(
        self,
        resource_type,
        name,
        configs=None,
    ):
        if not isinstance(resource_type, (ConfigResourceType)):
            resource_type = ConfigResourceType[str(resource_type).upper()]
        self.resource_type = resource_type
        self.name = name
        self.configs = configs
