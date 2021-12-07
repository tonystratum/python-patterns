class Resort:
    def __init__(self, name: str = "", price: float = .0, feature_ids: set = set(), environment_ids: set = set()):
        self.name: str = name
        self.price: float = price
        self.feature_ids: set = feature_ids
        self.environment_ids: set = environment_ids


class ResortBuilder:
    def __init__(self):
        self.resort = Resort()

    def reset(self) -> Resort:
        self.resort = Resort()
        return self.resort

    def get_object(self) -> Resort:
        return self.resort

    def set_name(self, name: str = ""):
        self.resort.name = name

    def set_price(self, price: float = .0):
        self.resort.price = price

    def add_feature_ids(self, feature_ids: list):
        for feature_id in feature_ids:
            self.resort.feature_ids.add(feature_id)

    def clear_feature_ids(self):
        self.resort.feature_ids = set()

    def add_environments(self, environments: list):
        for environment in environments:
            self.resort.environment_ids.add(environment)

    def clear_environments(self):
        self.resort.environment_ids = set()
