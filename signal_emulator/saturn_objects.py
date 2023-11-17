from dataclasses import dataclass
from signal_emulator.controller import BaseCollection, BaseItem


@dataclass(eq=False)
class PhaseToSaturnTurn(BaseItem):
    controller_key: str
    phase_ref: str
    saturn_a_node: int
    saturn_b_node: int
    saturn_c_node: int
    signal_emulator: object

    def get_key(self):
        return self.controller_key, self.phase_ref

    @property
    def controller(self):
        return self.signal_emulator.controllers.get_by_key(self.controller_key)

    @property
    def phase(self):
        return self.signal_emulator.phases.get_by_key(self.get_key())


class PhaseToSaturnTurns(BaseCollection):
    ITEM_CLASS = PhaseToSaturnTurn
    TABLE_NAME = "phase_to_saturn_turns"
    WRITE_TO_DATABASE = True

    def __init__(self, item_data, signal_emulator):
        super().__init__(item_data=item_data, signal_emulator=signal_emulator)


