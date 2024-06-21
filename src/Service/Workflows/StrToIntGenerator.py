from typing import Dict


class StrToIntGenerator:
    map: Dict[str, int] = {}
    counter: int = 0

    def get_int(self, string: any) -> int:
        string: str = str(string)
        if string in self.map:
            return self.map[string]

        self.counter += 1
        self.map[string] = self.counter

        return self.counter
