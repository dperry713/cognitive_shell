class DesiredState:
    def __init__(self):
        self.version = 0
        self.spec = {
            "tasks": [],
            "priority": "normal",
            "replicas": 3
        }

    def update(self, new_spec):
        self.version += 1
        self.spec = new_spec
        return self.version
