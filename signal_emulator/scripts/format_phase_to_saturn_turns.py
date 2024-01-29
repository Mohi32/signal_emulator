from signal_emulator.emulator import SignalEmulator
from signal_emulator.utilities.utility_functions import load_json_to_dict


def main(config_path, visum_signal_groups_att_path):
    config = load_json_to_dict(json_file_path=config_path)
    signal_emulator = SignalEmulator(config=config)
    signal_emulator.phase_to_saturn_turns.load_from_att_file(visum_signal_groups_att_path)


if __name__ == "__main__":
    config_path = "signal_emulator/resources/configs/signal_emulator_from_files_config.json"
    visum_signal_groups_att_path = "signal_emulator/resources/phase_to_saturn_turns/signal_groups_for_saturn.att"
    main(config_path, visum_signal_groups_att_path)