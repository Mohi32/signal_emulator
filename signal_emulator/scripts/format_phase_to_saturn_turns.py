from signal_emulator.emulator import SignalEmulator
from signal_emulator.utilities.utility_functions import load_json_to_dict


def main(config_path, visum_signal_groups_att_path):
    config = load_json_to_dict(json_file_path=config_path)
    signal_emulator = SignalEmulator(config=config)
    signal_emulator.phase_to_saturn_turns.add_from_att_file(visum_signal_groups_att_path)


if __name__ == "__main__":
    config_path = ""
    visum_signal_groups_att_path = ""
    main(config_path, visum_signal_groups_att_path)