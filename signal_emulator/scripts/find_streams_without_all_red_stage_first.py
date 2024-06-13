from signal_emulator.emulator import SignalEmulator
from signal_emulator.utilities.utility_functions import load_json_to_dict


def run(config_path):
    config = load_json_to_dict(json_file_path=config_path)
    signal_emulator = SignalEmulator(config=config)
    codes = signal_emulator.find_streams_without_all_red_stage_first()
    print(codes)


if __name__ == "__main__":
    run(config_path="signal_emulator/resources/configs/signal_emulator_from_files_config.json")
