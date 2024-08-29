from src.Api import Api

class UCDMMappingResolver:
    api: Api

    def __init__(self, api: Api):
        self.api = api

    def resolve(self, runner_message_id: int):
        pass
