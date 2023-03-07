from src.Message.BaseMessage import BaseMessage


class CreateFolder(BaseMessage):
    type: str

    def decode(self, data: {}):
        raise NotImplementedError("Please Implement this method")
