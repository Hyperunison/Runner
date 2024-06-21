class SerialGenerator:
    counter: int = 0

    def get_next_value(self) -> int:
        self.counter += 1
        return self.counter
