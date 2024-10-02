class OrderByDTO:
    field_name: str
    direction: str

    def __init__(self, field_name: str, direction: str):
        self.field_name = field_name
        self.direction = direction

        if self.if_direction_valid(direction):
            raise "Invalid operation"

    def if_direction_valid(self, direction: str) -> bool:
        valid_directions = ["asc", "desc"]

        return direction in valid_directions