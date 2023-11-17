import os

import pandas as pd
import pytest
from pytz import UTC

from signal_emulator.file_parsers.signal_event_parser import SignalEventParser


@pytest.fixture(scope="module")
def signal_event_parser():
    """
    Fixture for initialising the signal event parser
    :return: signal event parser object
    """
    # Perform common setup actions here, if needed
    # This fixture can be reused across multiple test functions
    current_directory = os.path.dirname(os.path.abspath(__file__))
    parent_directory = os.path.dirname(current_directory)
    os.chdir(parent_directory)
    signal_event_parser = SignalEventParser()
    yield signal_event_parser


@pytest.mark.usefixtures("signal_event_parser")
def test_signal_event_parser_end_to_end(signal_event_parser):
    """
    Test to validate the output of the signal event parser against a set of M37 messages extracted for the same day
    :param signal_event_parser: signal event parser object
    :return: None
    """
    signal_event_parser.process_signal_event_folder(
        signal_event_folder="tests/resources/signal_events", output_folder="tests/resources/M37"
    )
    validation_df = pd.read_csv("tests/resources/M37/M37_20230510_0800_0810.csv")
    test_df = pd.read_csv("tests/resources/M37/M37_20230510_CNTR.csv")
    validation_df["SiteIdTrimmed"] = validation_df["SiteId"].str[1:]
    validation_df["dt"] = pd.to_datetime(validation_df["UtcDateTimestamp"])
    test_df["green_time"] = test_df["green_time"].astype(int)
    test_df["interstage_time"] = test_df["interstage_time"].astype(int)
    joined_df = test_df.merge(
        validation_df,
        how="left",
        right_on=("UtcDateTimestamp", "SiteIdTrimmed", "UtcStageId"),
        left_on=("timestamp", "site_id", "utc_stage_id"),
    )
    # pass if both green time and interstage time match the validation data
    joined_df["pass"] = (joined_df["green_time"] == joined_df["Gn"]) & (
        joined_df["interstage_time"] == joined_df["Ig"]
    )
    joined_df["dt"] = pd.to_datetime(joined_df["timestamp"])
    start_datetime = (
        pd.to_datetime("2023-05-10T08:01:00Z", format="%Y-%m-%dT%H:%M:%SZ", utc=True)
        .tz_localize(None)
        .tz_localize(UTC)
    )
    end_datetime = (
        pd.to_datetime("2023-05-10T08:09:00Z", format="%Y-%m-%dT%H:%M:%SZ", utc=True)
        .tz_localize(None)
        .tz_localize(UTC)
    )
    filtered_df = joined_df[(joined_df["dt"] >= start_datetime) & (joined_df["dt"] <= end_datetime)]
    pass_rate = filtered_df["pass"].sum() / len(filtered_df)
    # overall pass rate of 95%
    assert pass_rate > 0.95
    error_counts = filtered_df[~filtered_df["pass"]].groupby(["site_id", "utc_stage_id"]).count()
    all_counts = filtered_df.groupby(["site_id", "utc_stage_id"]).count()
    error_rates = error_counts / all_counts
    filtered_df = filtered_df.dropna()
    averaged_df = filtered_df.groupby(["site_id", "utc_stage_id"]).agg(
        {
            "green_time": "mean",
            "Gn": "mean",
            "interstage_time": "mean",
            "Ig": "mean",
        }
    )
    averaged_df["green_abs_diff"] = abs(averaged_df["green_time"] - averaged_df["Gn"])
    averaged_df["interstage_abs_diff"] = abs(averaged_df["interstage_time"] - averaged_df["Ig"])
    # pass if averaged green time and interstage times are within 1 second of the validation data
    averaged_df["pass"] = (averaged_df["green_abs_diff"] <= 1) & (
        averaged_df["interstage_abs_diff"] <= 1
    )
    pass_rate = averaged_df["pass"].sum() / len(averaged_df)
    averaged_df.sort_values(by="green_abs_diff", ascending=False, inplace=True)
    # overall pass rate of 97%
    assert pass_rate > 0.97
