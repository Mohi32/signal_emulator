import os
from collections import Counter

from signal_emulator.controller import Controller
from signal_emulator.emulator import SignalEmulator
from signal_emulator.utilities.utility_functions import load_json_to_dict


class TimingSheetErrors:
    def __init__(self, config):
        self.signal_emulator = SignalEmulator(config=config)

    def timing_sheet_iterator(self, timing_sheet_directory_path):
        for filename in os.listdir(timing_sheet_directory_path):
            timing_sheet_path = os.path.join(timing_sheet_directory_path, filename)
            if os.path.exists(
                os.path.join(
                    f"{timing_sheet_directory_path}_fixed", filename.replace(".csv", "_fixed.csv")
                )
            ):
                timing_sheet_path = os.path.join(
                    f"{timing_sheet_directory_path}_fixed", filename.replace(".csv", "_fixed.csv")
                )
            if (
                filename.endswith("csv")
                and os.path.isfile(timing_sheet_path)
                and "_Junc" in filename
            ):
                if Controller.validate_timing_sheet_csv(timing_sheet_path):
                    controller = Controller.init_from_timing_sheet_csv(
                        timing_sheet_csv_path=timing_sheet_path,
                        output_timing_sheet_json=False,
                        signal_emulator=self,
                    )
                    if not controller.is_parallel():
                        yield controller

    def find_repeated_stage_names(self):
        for controller in self.signal_emulator.controllers:
            stream_stage_names = [
                (stage.stream_number, stage.stage_name) for stage in controller.stages
            ]
            count = Counter(stream_stage_names)
            stage_names_repeated = [item for item, freq in count.items() if freq > 1]
            if len(stage_names_repeated) > 0:
                print(controller.site_number, stage_names_repeated)

    def find_invalid_phase_delays(self):
        for controller in self.signal_emulator.controllers:
            for phase_delay in list(controller.phase_delays):
                if (
                    phase_delay.phase_ref not in phase_delay.end_stage.phase_keys_in_stage
                    and phase_delay.phase_ref not in phase_delay.start_stage.phase_keys_in_stage
                ):
                    print(
                        f"Site {controller.site_number} {phase_delay.get_key()}: "
                        f"End stage {phase_delay.end_stage_key} "
                        f"Start stage {phase_delay.start_stage_key} "
                        f"Phase ref {phase_delay.phase_ref} "
                        f"Phase delay {phase_delay.delay_time}, "
                        f"phase ref not found in end stage or start stage"
                    )


if __name__ == "__main__":
    signal_emulator_config = load_json_to_dict(
        json_file_path="signal_emulator/resources/configs/signal_emulator_empty_config.json"
    )
    tse = TimingSheetErrors(config=signal_emulator_config)
    tse.signal_emulator.load_timing_sheets_from_directory("signal_emulator/resources/timing_sheets")
    tse.find_repeated_stage_names()
    tse.find_invalid_phase_delays()
