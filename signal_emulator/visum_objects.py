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
        output_data = []
        for item in self:
            if item.time_period_id == time_period.name:
                output_data.append([getattr(item, attr_name) for attr_name in self.COLUMNS.values()])
        if self.TABLE_NAME == "visum_signal_groups":
            output_data = sorted(output_data, key=lambda k: (k[0], k[1]))
        elif self.TABLE_NAME == "visum_signal_controllers":
            output_data = sorted(output_data, key=lambda k: k[0])
        else:
            raise ValueError
        output_data = copy(self.OUTPUT_HEADER) + [self.add_column_header()] + output_data
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
    controller_key: str
    phase_ref: str
    phase_number: int
    phase_name: str
    green_time_start: int
    green_time_end: int
    time_period_id: str
    source_data: str
    signal_emulator: object

    def get_key(self):
        return self.controller_key, self.phase_name, self.time_period_id

    def get_phase_key(self):
        return self.controller_key, self.phase_ref

    @property
    def phase(self):
        return self.signal_emulator.phases.get_by_key(self.get_phase_key())

    @property
    def signal_controller_number(self):
        return self.visum_signal_controller.signal_controller_number

    @property
    def visum_signal_controller(self):
        return self.signal_emulator.visum_signal_controllers.get_by_key((self.controller_key, self.time_period_id))

    @property
    def phase_type(self):
        return self.phase.phase_type.name

    @property
    def associated_phase_ref(self):
        return self.phase.associated_phase_ref

    @property
    def phase_termination_type(self):
        return self.phase.termination_type.name

    @property
    def phase_appearance_type(self):
        return self.phase.appearance_type_int


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
        "SOURCE_DATA": "source_data",
        "PHASE_TYPE": "phase_type",
        "ASSOCIATED_PHASE_REF": "associated_phase_ref",
        "PHASE_TERMINATION_TYPE": "phase_termination_type",
        "PHASE_APPEARANCE_TYPE": "phase_appearance_type"
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
            controller_key=phase_timing.controller_key,
            phase_ref=phase_timing.phase.phase_ref,
            phase_number=phase_timing.signal_group_number,
            phase_name=phase_timing.visum_phase_name,
            green_time_start=phase_timing.start_time,
            green_time_end=phase_timing.end_time,
            time_period_id=phase_timing.time_period_id,
            source_data=phase_timing.signal_emulator.run_datestamp,
            signal_emulator=self.signal_emulator
        )
        self.data[visum_signal_group.get_key()] = visum_signal_group


@dataclass(eq=False)
class VisumSignalController:
    DEFAULT_SIGNALISATION_TYPE = "SIGNALIZATIONVISSIG"
    controller_key: str
    name: str
    cycle_time: int
    time_period_id: str
    source_data: str
    signal_emulator: object
    signalisation_type: Optional[str] = DEFAULT_SIGNALISATION_TYPE

    def get_key(self):
        return self.controller_key, self.time_period_id

    @property
    def code(self):
        a, b = divmod(self.signal_controller_number, 1000)
        return f"{a:02}/{b:06}"

    @property
    def signal_controller_number(self):
        parts = self.controller_key.split("/")
        if parts[0][0].isalpha():
            parts[0] = parts[0][1:]
        return int(parts[0]) * 1000 + int(parts[1])

    @property
    def controller(self):
        return self.signal_emulator.controllers.get_by_key(self.controller_key)

    @property
    def google_maps_url(self):
        return f"https://www.google.com/maps/place/{self.controller.latitude},{self.controller.longitude}"


class VisumSignalControllers(VisumCollection):
    COLUMNS = {
        "NO": "signal_controller_number",
        "CYCLETIME": "cycle_time",
        "SIGNALIZATIONTYPE": "signalisation_type",
        "SOURCE_DATA": "source_data",
        "CODE": "code",
        "NAME": "name",
        "GOOGLE_MAPS_URL": "google_maps_url"
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

    def add_visum_signal_controller(self, controller_key, name, cycle_time, time_period_id, source_data):
        signal_controller = VisumSignalController(controller_key, name, cycle_time, time_period_id, source_data, signal_emulator=self.signal_emulator)
        self.data[signal_controller.get_key()] = signal_controller
