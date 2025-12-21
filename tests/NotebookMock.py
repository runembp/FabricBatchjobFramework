from unittest.mock import MagicMock

class MockNotebookUtils:
    def __init__(self):
        self.fs = MagicMock()
        self.base_path = ""

    def get_local_path(self, fabric_path):
        return fabric_path.replace("Files", self.base_path)