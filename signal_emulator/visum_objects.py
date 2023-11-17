import os
from copy import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from signal_emulator.controller import BaseCollection, BaseItem
from signal_emulator.utilities.utility_functions import list_to_csv


class VisumCollection(BaseCollection):
    OUTPUT_HEADER = [
        ["$VISION"],
        ["$VERSION:VERSNR", "FILETYPE", "LANGUAGE", "UNIT"],
        ["13.000", "Net", "ENG", "KM"],
        [],
    ]
    COLUMNS = {}
    VISUM_TABLE_NAME = None

    def __init__(self, item_data, signal_emulator, output_directory):
        super().__init__(
            item_data=item_data
        )
        self.signal_emulator = signal_emulator
        self.output_directory = output_directory

    def export_to_net_files(self, time_periods=None):
        if time_periods is None:
            time_periods = self.signal_emulator.time_periods.get_all()
        for time_period in time_periods:
            self.export_to_net_file(time_period)

    def export_to_net_file(self, time_period, output_path=None):
        if not output_path:
            output_path = os.path.join(
                self.output_directory,
                f"VISUM_{self.VISUM_TABLE_NAME}_{time_period.name}.net",
            )
        output_data = copy(self.OUTPUT_HEADER)
        output_data.append(self.add_column_header())
        for item in self:
            if item.time_period_id == time_period.name:
                output_data.append([getattr(item, attr_name) for attr_name in self.COLUMNS.values()])
        Path(output_path).parent.mkdir(exist_ok=True, parents=True)
        list_to_csv(output_data, output_path, delimiter=";")
        self.signal_emulator.logger.info(
            f"VISUM {self.VISUM_TABLE_NAME} output to net file: {output_path}"
        )

    def add_column_header(self):
        return [
            a if i > 0 else f"${self.VISUM_TABLE_NAME}:{a}" for i, a in enumerate(self.COLUMNS.keys())
        ]


@dataclass(eq=False)
class VisumSignalGroup(BaseItem):
    signal_controller_number: int
    phase_number: int
    phase_name: str
    green_time_start: int
    green_time_end: int
    time_period_id: str

    def get_key(self):
        return self.signal_controller_number, self.phase_name, self.time_period_id


class VisumSignalGroups(VisumCollection):
    ITEM_CLASS = VisumSignalGroup
    TABLE_NAME = "visum_signal_groups"
    WRITE_TO_DATABASE = True

    COLUMNS = {
        "SCNO": "signal_controller_number",
        "NO": "phase_number",
        "NAME": "phase_name",
        "GTSTART": "green_time_start",
        "GTEND": "green_time_end",
    }
    VISUM_TABLE_NAME = "SIGNALGROUP"

    def __init__(self, item_data, signal_emulator, output_directory):
        super().__init__(
            item_data=item_data,
            signal_emulator=signal_emulator,
            output_directory=output_directory,
        )
        self.signal_emulator = signal_emulator

    def add_from_phase_timing(self, phase_timing):
        visum_signal_group = VisumSignalGroup(
            signal_controller_number=phase_timing.controller.site_number_int,
            phase_number=phase_timing.signal_group_number,
            phase_name=phase_timing.visum_phase_name,
            green_time_start=phase_timing.start_time,
            green_time_end=phase_timing.end_time,
            time_period_id=phase_timing.time_period_id
        )
        self.data[visum_signal_group.get_key()] = visum_signal_group


@dataclass(eq=False)
class VisumSignalController:
    DEFAULT_SIGNALISATION_TYPE = "SIGNALIZATIONVISSIG"
    signal_controller_number: int
    cycle_time: int
    time_period_id: str
    signalisation_type: Optional[str] = DEFAULT_SIGNALISATION_TYPE

    def get_key(self):
        return self.signal_controller_number, self.time_period_id


class VisumSignalControllers(VisumCollection):
    COLUMNS = {
        "NO": "signal_controller_number",
        "CYCLETIME": "cycle_time",
        "SIGNALIZATIONTYPE": "signalisation_type",
    }
    ITEM_CLASS = VisumSignalController
    TABLE_NAME = "visum_signal_controllers"
    WRITE_TO_DATABASE = True
    VISUM_TABLE_NAME = "SIGNALCONTROL"

    def __init__(self, item_data, signal_emulator, output_directory):
        super().__init__(
            item_data=item_data,
            signal_emulator=signal_emulator,
            output_directory=output_directory,
        )
        self.signal_emulator = signal_emulator

    def add_visum_signal_controller(self, signal_controller_number, cycle_time, time_period_id):
        signal_controller = VisumSignalController(signal_controller_number, cycle_time, time_period_id)
        self.data[signal_controller.get_key()] = signal_controller
