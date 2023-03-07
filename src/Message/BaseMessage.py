class BaseMessage:
    type: str
    folder: str

    def decode(self, data: {}):
        self.folder = data.folder
