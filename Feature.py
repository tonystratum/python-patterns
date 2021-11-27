class Feature:
    def __init__(self, name: str = ""):
        self.name = name


class FeatureBuilder:
    def __init__(self):
        self.feature = Feature()

    def reset(self) -> Feature:
        self.feature = Feature()
        return self.feature

    def get_object(self) -> Feature:
        return self.feature

    def set_name(self, name: str = ""):
        self.feature.name = name
