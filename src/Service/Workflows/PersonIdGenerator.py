from typing import Dict


class PersonIdGenerator:
    map: Dict[str, int] = {}
    counter: int = 1

    def get_person_id_int(self, person_id: any) -> int:
        person_id_str: str = str(person_id)
        if person_id_str in self.map:
            return self.map[person_id_str]

        self.counter += 1
        self.map[person_id_str] = self.counter

        return self.counter
