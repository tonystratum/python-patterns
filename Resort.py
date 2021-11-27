class Resort:
    def __init__(self, name: str = "", price: float = .0, feature_ids: set = set(), environment_ids: set = set()):
        self.name: str = name
        self.price: float = price
        self.feature_ids: set = feature_ids
        self.environment_ids: set = environment_ids


class ResortBuilder:
    def __init__(self):
        self.game = Resort()

    def reset(self) -> Resort:
        self.game = Resort()
        return self.game

    def get_object(self) -> Resort:
        return self.game

    def set_name(self, name: str = ""):
        self.game.name = name

    def set_price(self, price: float = .0):
        self.game.price = price

    def add_feature_ids(self, feature_ids: list):
        for feature_id in feature_ids:
            self.game.feature_ids.add(feature_id)

    def clear_feature_ids(self):
        self.game.feature_ids = set()

    def add_environments(self, environments: list):
        for environment in environments:
            self.game.environment_ids.add(environment)

    def clear_environments(self):
        self.game.environment_ids = set()
