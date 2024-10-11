import glob
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from signal_emulator.time_period import TimePeriods
from signal_emulator.utilities.utility_functions import load_json_to_dict


class ConnectPlusTimingsProcessor:
    COLUMN_LIMITS_2 = [(0, 8), (9, 13), (14, 19), (23, 26), (27, 32), (37, 41)]
    COLUMN_LIMITS_3 = [(0, 8), (9, 13), (14, 19), (23, 26), (27, 32), (35, 39), (40, 46), (48, 53)]
    COLUMN_LIMITS_4 = [(0, 8), (9, 13), (14, 19), (23, 26), (27, 32), (35, 39), (40, 46), (49, 52), (53, 58), (62, 66)]
    COLUMN_LIMITS_5 = [(0, 8), (9, 13), (14, 19), (23, 26), (27, 32), (35, 39), (40, 46), (49, 52), (53, 58), (61, 65), (66, 71), (75, 79)]
    COLUMN_LIMITS_6 = [(0, 8), (9, 13), (14, 19), (23, 26), (27, 32), (35, 39), (40, 46), (49, 52), (53, 58), (61,65), (66,71), (74,78), (79,84), (88, 92)]
    HEADER_ROWS = 5
    COLUMN_NAMES_2 = ["timestamp", "stage_1_ig", "stage_1_gt", "stage_2_ig", "stage_2_gt", "cycle_time"]
    COLUMN_NAMES_3 = ["timestamp", "stage_1_ig", "stage_1_gt", "stage_2_ig", "stage_2_gt", "stage_3_ig", "stage_3_gt","cycle_time"]
    COLUMN_NAMES_4 = ["timestamp", "stage_1_ig", "stage_1_gt", "stage_2_ig", "stage_2_gt", "stage_3_ig", "stage_3_gt", "stage_4_ig", "stage_4_gt", "cycle_time"]
    COLUMN_NAMES_5 = ["timestamp", "stage_1_ig", "stage_1_gt", "stage_2_ig", "stage_2_gt", "stage_3_ig", "stage_3_gt", "stage_4_ig", "stage_4_gt", "stage_5_ig", "stage_5_gt", "cycle_time"]
    COLUMN_NAMES_6 = ["timestamp", "stage_1_ig", "stage_1_gt", "stage_2_ig", "stage_2_gt", "stage_3_ig", "stage_3_gt", "stage_4_ig", "stage_4_gt", "stage_5_ig", "stage_5_gt", "stage_6_ig", "stage_6_gt", "cycle_time"]

    BASE_DIRECTORY = os.path.dirname(__file__)
    DEFAULT_TIME_PERIODS_PATH = os.path.join(BASE_DIRECTORY, "../resources/time_periods/default_time_periods.json")

    def __init__(self):
        self.time_periods = TimePeriods(load_json_to_dict(self.DEFAULT_TIME_PERIODS_PATH))
        self.all_timings_df = None

    def check_files(self, connect_plus_directory):
        file_data = []
        for timings_file_path in self.timings_directory_iterator(connect_plus_directory):
            datetime_obj = self.get_datetime_from_file_path(timings_file_path)
            this_timings_df = pd.read_fwf(
                timings_file_path,
                colspecs=self.COLUMN_LIMITS,
                skiprows=self.HEADER_ROWS,
                names=self.COLUMN_NAMES,
            )
            this_timings_df = this_timings_df.dropna()
            node_id, site_id = self.get_controller_key_from_timings_file(timings_file_path)
            file_data.append([os.path.basename(timings_file_path), node_id, site_id, datetime_obj, len(this_timings_df.index)])
        file_dat_df = pd.DataFrame(file_data)
        file_dat_df.columns = ["filename", "node_id", "stream_id", "date", "row_count"]
        file_dat_df["is_tues_to_thurs"] = file_dat_df["date"].dt.weekday.between(1,3)
        summary_df = file_dat_df.groupby("node_id").agg(
            stream_count = ("stream_id", pd.Series.nunique),
            file_count = ("filename", pd.Series.nunique),
            file_count_tues_to_thurs = ("is_tues_to_thurs", "sum"),
            average_row_count = ("row_count", "mean"),
            min_date = ("date", min),
            max_date=("date", max)
        )
        summary_df.to_csv("../resources/connect_plus_signal_timings_summary.csv")

    def load_data_from_connect_plus_directory(self, connect_plus_directory, weekday_indices=None, connect_plus_junction_groups_path=None):
        connect_plus_junction_groups = load_json_to_dict(connect_plus_junction_groups_path)
        junction_group_dict = {}
        for junction_group in connect_plus_junction_groups["junction_groups"]:
            for controller_key in junction_group["controller_keys"]:
                junction_group_dict[controller_key] = junction_group["junction_group_name"]
        all_timings_df = pd.DataFrame()
        for timings_file_path in self.timings_directory_iterator(connect_plus_directory, weekday_indices):
            this_timings_df = self.load_data_from_connect_plus_file(timings_file_path)
            all_timings_df = pd.concat([all_timings_df, this_timings_df], ignore_index=True)

        all_timings_df["intergreen_time"] = all_timings_df["intergreen_time"].astype(int)
        all_timings_df["green_time"] = all_timings_df["green_time"].astype(int)
        all_timings_df.set_index("timestamp", inplace=True)
        all_timings_df['group_id'] = all_timings_df['node_id'].map(junction_group_dict)
        self.all_timings_df = all_timings_df

    def load_data_from_connect_plus_file(self, timings_file_path):
        print(f"loading timings file: {timings_file_path}")
        node_id, site_id = self.get_controller_key_from_timings_file(timings_file_path)
        datetime_obj = self.get_datetime_from_file_path(timings_file_path)
        num_stages = self.get_num_stages_from_file_path(timings_file_path)
        this_timings_df = pd.read_fwf(
            timings_file_path,
            colspecs=getattr(self, f"COLUMN_LIMITS_{num_stages}"),
            skiprows=self.HEADER_ROWS,
            names=getattr(self, f"COLUMN_NAMES_{num_stages}"),
        )
        this_timings_df["node_id"] = node_id
        this_timings_df["site_id"] = site_id

        stages_df_list = []
        for stage_num in range(1, num_stages + 1):
            stage_df = this_timings_df.copy()[["timestamp", "node_id", "site_id", f"stage_{stage_num}_ig", f"stage_{stage_num}_gt"]]
            stage_df["stage_number"] = stage_num
            stage_df.rename(
                inplace=True,
                columns={
                    f"stage_{stage_num}_ig": "intergreen_time",
                    f"stage_{stage_num}_gt": "green_time"
                }
            )
            stages_df_list.append(stage_df)

        all_stages_df = pd.concat(stages_df_list, ignore_index=True)
        all_stages_df[["intergreen_time", "green_time"]] = all_stages_df[["intergreen_time", "green_time"]].fillna(0)
        if len(all_stages_df.index):
            all_stages_df[["intergreen_time", "green_time"]] = all_stages_df[["intergreen_time", "green_time"]].apply(pd.to_numeric, errors='coerce')
            all_stages_df = all_stages_df.dropna()
            all_stages_df = all_stages_df[all_stages_df["timestamp"] != "TOTALS:"]
        all_stages_df["timestamp"] = pd.to_datetime(all_stages_df["timestamp"], format="%H:%M:%S")
        timedelta =  datetime_obj.date() - datetime(1900, 1, 1).date()
        all_stages_df["timestamp"] = all_stages_df["timestamp"] + timedelta
        return all_stages_df

    @staticmethod
    def get_controller_key_from_timings_file(timings_file_path):
        with open(timings_file_path, 'r') as file:
            first_line = file.readline()
            second_line = file.readline()
        if "Observation of" in first_line:
            line = first_line
        elif "Observation of" in second_line:
            line = second_line
        else:
            raise ValueError
        stream_key = line.split(" ")[2]
        part_1 = stream_key[2:4]
        part_2 = stream_key[4:7]
        return f"J{part_1}/{part_2[:-1]}0", f"J{part_1}/{part_2}"

    def calculate_average_signal_timings(self):
        """
        Function to return a DataFrame of averaged M37 signal timings
        :return: DataFrame of M37 timings
        """
        m37_all = pd.DataFrame()
        for time_period in self.time_periods:
            # filter M37s to the time bounds of the time Period
            m37_filtered = self.all_timings_df.between_time(
                start_time=time_period.start_time_str, end_time=time_period.end_time_str
            )
            # Group by NodeId, site_id and utc_stage_id
            m37_grouped_node_site_stage = m37_filtered.groupby(
                ["group_id", "node_id", "site_id", "stage_number"]
            )
            # aggregate to get the number of occurrences, average green time and total green times
            m37 = m37_grouped_node_site_stage.agg(
                occurrences=("intergreen_time", "count"),
                interstage_total=("intergreen_time", "sum"),
                interstage_average=("intergreen_time", "mean"),
                green_total=("green_time", "sum"),
                green_average=("green_time", "mean"),
            )
            m37["start_timestamp"] = time_period.start_time
            m37["end_timestamp"] = time_period.end_time
            m37["period_id"] = time_period.name
            m37["stage_total"] = m37["green_total"] + m37["interstage_total"]
            m37_cycles = (
                m37_grouped_node_site_stage.size().groupby(["node_id", "site_id"]).agg(cycles="max")
            )
            m37 = m37.merge(m37_cycles, left_index=True, right_index=True, how="outer")
            total_times = m37.groupby(["node_id", "site_id"]).agg(total_time=("stage_total", "sum"))
            total_times["coverage"] = total_times["total_time"] / time_period.total_seconds
            m37 = m37.merge(total_times, left_index=True, right_index=True, how="outer")
            m37.reset_index(inplace=True)

            stream_cycle_time = m37.groupby(["node_id", "site_id"]).agg(
                green_time_sum=("green_average", "sum"),
                interstage_time_sum=("interstage_average", "sum")
            )
            # sum the adjusted green times and interstage times to get the cycle time
            stream_cycle_time["stream_cycle_time"] = (
                stream_cycle_time["green_time_sum"]
                + stream_cycle_time["interstage_time_sum"]
            )
            m37 = m37.merge(
                stream_cycle_time["stream_cycle_time"],
                on=["node_id", "site_id"],
                how="left",
            )

            node_cycle_time = m37.groupby(["node_id"])["stream_cycle_time"].apply(
                lambda x: self.get_node_cycle_time(x)
            ).to_frame(name='node_cycle_time')
            m37 = m37.merge(
                node_cycle_time["node_cycle_time"],
                on="node_id",
                how="left",
            )

            group_cycle_time = m37.groupby(["group_id"])["stream_cycle_time"].apply(
                lambda x: self.get_node_cycle_time(x)
            ).to_frame(name='group_cycle_time')
            m37 = m37.merge(
                group_cycle_time["group_cycle_time"],
                on="group_id",
                how="left",
            )

            # adjust green times by node cycle time over stream cycle time
            m37["cycle_time_factor"] = m37["group_cycle_time"] / m37["stream_cycle_time"]
            m37["adjusted_green_average"] = m37["green_average"] * m37["cycle_time_factor"]
            m37["adjusted_interstage_average"] = m37["interstage_average"] * m37["cycle_time_factor"]

            m37.reset_index(inplace=True)
            m37.set_index("node_id", inplace=True)
            m37.reset_index(inplace=True)
            m37.set_index(["node_id", "site_id", "stage_number"], inplace=True)
            # unstack the utc_stage_id from the row index to a column so that the stage times can be rounded together
            m37_unstack = m37[["adjusted_green_average", "adjusted_interstage_average"]].unstack(level=2)
            # apply rounding function that rounds times to whole seconds,
            # while maintaining the total (cycle time)
            m37_unstack = m37_unstack.apply(lambda x: self.rounding_function(x), axis=1)
            m37_stack = m37_unstack.stack()
            m37_stack.rename(
                columns={
                    "adjusted_green_average": "final_green_time",
                    "adjusted_interstage_average": "final_interstage_time",
                },
                inplace=True,
            )
            m37 = m37.merge(m37_stack, left_index=True, right_index=True, how="outer")

            m37.reset_index(inplace=True)
            m37_all = pd.concat([m37_all, m37], ignore_index=True)
            print(m37_all[m37_all["final_green_time"].isna()])
            m37_all["green_time"] = m37_all["final_green_time"].astype(int)
            m37_all["interstage_time"] = m37_all["final_interstage_time"].astype(int)
            m37_all["cycle_time"] = m37_all["group_cycle_time"].astype(int)
        m37_all["utc_stage_id"] = "G" + m37_all['stage_number'].astype(str)
        m37_all = m37_all[
            [
                "node_id",
                "site_id",
                "utc_stage_id",
                "stage_number",
                "period_id",
                "green_time",
                "interstage_time",
                "cycle_time",
            ]
        ]
        return m37_all

    @staticmethod
    def get_datetime_from_file_path(file_path):
        with open(file_path, 'r') as file:
            first_line = file.readline()
            second_line = file.readline()
        if "Observation of" in first_line:
            line = first_line
        elif "Observation of" in second_line:
            line = second_line
        else:
            raise ValueError
        date_str = line.split()[5]
        try:
            dt_obj = datetime.strptime(date_str, "%d-%b-%y")
        except:
            raise Exception
        return dt_obj

    def timings_directory_iterator(self, connect_plus_directory, weekday_indices=None):
        for junction_directory in glob.glob(os.path.join(connect_plus_directory, '*/')):
            clean_directory = Path(junction_directory).as_posix()
            if clean_directory[-3:] == "OLD":
                continue
            timings_files = glob.glob(os.path.join(clean_directory, "Signal Timings", '*.txt'))
            for file in timings_files:
                if weekday_indices:
                    if self.get_datetime_from_file_path(file).weekday() in weekday_indices:
                        yield Path(file).as_posix()
                else:
                    yield Path(file).as_posix()

    @staticmethod
    def rounding_function(original_series):
        """
        Rounding function that rounds a set of values to whole seconds while maintaining
        the original total
        :param original_series:
        :return: Series of rounded values
        """

        def custom_round(value):
            """
            custom rounding function that ignores NaNs
            :param value: value
            :return: rounded value or NaN
            """

            if pd.notna(value):
                return round(value)
            else:
                return value

        # Calculate the original total sum
        original_total = int(round(original_series.sum(), 4))
        # Round the values
        rounded_series = original_series.apply(custom_round)
        rounded_total = int(rounded_series.sum())
        # Calculate the difference between rounded total and original total
        difference = original_total - rounded_total
        # Identify the indices with the largest differences
        indices_largest_diff = (
            (original_series - rounded_series).abs().nlargest(int(abs(difference))).index
        )
        # Adjust values at identified indices
        for index in indices_largest_diff:
            if difference > 0:
                rounded_series.at[index] += 1
            elif difference < 0:
                rounded_series.at[index] -= 1
        return rounded_series

    def get_num_stages_from_file_path(self, file_path):
        with open(file_path, 'r') as file:
            for line in file.readlines():
                if "STAGE" in line:
                    break
        return line.count("STAGE")

    @staticmethod
    def get_node_cycle_time(x):
        low_cts = x[(x > 30) & (x < 180)]
        if len(low_cts):
            return int(low_cts.mean().round(0))
        else:
            return int(x.mean().round(0))


if __name__=="__main__":
    cptp = ConnectPlusTimingsProcessor()
    cptp.load_data_from_connect_plus_directory(
        connect_plus_directory="../resources/connect_plus",
        connect_plus_junction_groups_path="../resources/connect_plus/junction_groups.json",
        weekday_indices=[1,2,3],
    )
    timings_df = cptp.calculate_average_signal_timings()
    timings_df.to_csv("../resources/M37/averaged/connect_plus_averaged_240925.csv", index=False)
