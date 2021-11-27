class Environment:
    def __init__(self, name: str = ""):
        self.name = name


class EnvironmentBuilder:
    def __init__(self):
        self.environment = Environment()

    def reset(self) -> Environment:
        self.environment = Environment()
        return self.environment

    def get_object(self) -> Environment:
        return self.environment

    def set_name(self, name: str = ""):
        self.environment.name = name
