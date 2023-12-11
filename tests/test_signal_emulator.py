import os

import pytest

from signal_emulator.emulator import SignalEmulator
from signal_emulator.utilities.utility_functions import load_json_to_dict, clean_site_number


@pytest.fixture(scope="module")
def signal_emulator():
    # Perform common setup actions here, if needed
    # This fixture can be reused across multiple test functions
    current_directory = os.path.dirname(os.path.abspath(__file__))
    parent_directory = os.path.dirname(current_directory)
    os.chdir(parent_directory)
    signal_emulator_config = load_json_to_dict(
        json_file_path="tests/resources/signal_emulator_empty_config.json"
    )
    signal_emulator = SignalEmulator(config=signal_emulator_config)
    yield signal_emulator


@pytest.mark.usefixtures("signal_emulator")
@pytest.mark.parametrize(
    "plan_path, timing_sheet_path, controller_key, stream_number, plan_number, expected_stage_sequence",
    [
        (
            "tests/resources/plans/j00004.pln",
            "tests/resources/timing_sheets/00_000004_Junc.csv",
            "J00/004",
            0,
            1,
            [1, 2, 3, 5],
        ),
        (
            "tests/resources/plans/j03193.pln",
            "tests/resources/timing_sheets/03_000193_Junc.csv",
            "J03/193",
            0,
            3,
            [1, 2, 3, 4, 5],
        ),
        (
            "tests/resources/plans/j00135.pln",
            "tests/resources/timing_sheets/00_000004_Junc.csv",
            "J00/004",
            1,
            1,
            [9, 8],
        ),
    ],
)
def test_stage_sequence(
    signal_emulator,
    timing_sheet_path,
    plan_path,
    controller_key,
    stream_number,
    plan_number,
    expected_stage_sequence,
):
    signal_emulator.load_timing_sheet_csv(timing_sheet_path)
    signal_emulator.load_plan_from_pln(plan_path)
    am_period = signal_emulator.time_periods.get_by_key("AM")
    stream = signal_emulator.streams.get_by_key((controller_key, stream_number))
    plan = signal_emulator.plans.get_by_key((stream.site_number, plan_number))
    signal_emulator.signal_plans.add_from_stream_plan_dict({stream: plan}, am_period, 1)
    signal_plan_stream = signal_emulator.signal_plan_streams.get_by_key(
        (controller_key, 1, stream.stream_number_linsig)
    )
    stage_sequence = [sps.stage_number for sps in signal_plan_stream.signal_plan_stages]
    assert stage_sequence == expected_stage_sequence


@pytest.mark.parametrize(
    "site_number_input, expected_output",
    [
        ("01/125", "J01/125"),
        ("J01/125", "J01/125"),
        ("J01/000125", "J01/125"),
        ("J01/000125/U", "J01/125"),
        ("01/000007/U", "J01/007"),
        ("P01/000007/U", "J01/007"),
    ],
)
def test_clean_site_number(site_number_input, expected_output):
    assert clean_site_number(site_number_input) == expected_output
