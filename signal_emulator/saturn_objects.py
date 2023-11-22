import csv
import os

from dataclasses import dataclass
from signal_emulator.controller import BaseCollection, BaseItem


@dataclass(eq=False)
class PhaseToSaturnTurn:
    signal_emulator: object
    controller_id: int
    phase: str
    turn: int
    saturn_a_node: int
    saturn_b_node: int
    saturn_c_node: int

    def get_key(self):
        return self.controller_id, self.phase, self.turn

class PhaseToSaturnTurns(BaseCollection):
    ITEM_CLASS = PhaseToSaturnTurn
    TABLE_NAME = "phase_to_saturn_turns"
    WRITE_TO_DATABASE = True

    def __init__(self, signal_emulator, saturn_lookup_file=None):
        super().__init__(item_data=[], signal_emulator=signal_emulator)
        self.signal_emulator = signal_emulator
        if saturn_lookup_file:
            self.load_from_saturn_lookup_file(saturn_lookup_file)

    def load_from_saturn_lookup_file(self, saturn_lookup_file):

        if os.path.exists(saturn_lookup_file):
            self.init_from_saturn_file(saturn_lookup_file)
        else:
            self.signal_emulator.logger.warning(f"SATURN lookup file: {saturn_lookup_file} does not exist")

    def init_from_saturn_file(self, saturn_lookup_file):
        with open(saturn_lookup_file, 'r') as csvfile:
            csvreader = csv.DictReader(csvfile)
            for row in csvreader:
                self.add_item(row, signal_emulator=self.signal_emulator)