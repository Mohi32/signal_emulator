import os
from dataclasses import dataclass

import pandas as pd

from signal_emulator.enums import M37StageToStageNumber
from signal_emulator.controller import BaseCollection
from signal_emulator.time_period import TimePeriods


@dataclass(eq=False)
class M37Average:
    """
    Class to represent M37 signal timing information for one node, site and stage combination
    """

    signal_emulator: object
    node_id: str
    site_number: str
    stage_id: str
    period_id: str
    green_time: int
    interstage_time: int
    cycle_time: int

    def __repr__(self):
        return (
            f"M37: {self.site_number=} {self.stage_id=} {self.green_time=} {self.interstage_time=}"
        )

    def get_key(self):
        return self.site_number, self.stage_id, self.period_id

    @staticmethod
    def clean_site_id(site_id):
        return site_id.replace("P", "J")

    @property
    def total_time(self):
        return self.green_time + self.interstage_time


class M37Averages(BaseCollection):
    """
    Class to represent a collection of M37 objects
    """

    TABLE_NAME = "m37_averages"
    ITEM_CLASS = M37Average
    WRITE_TO_DATABASE = True
    COLUMN_DTYPES = {"FinalGreenTime": int, "FinalInterstageTime": int, "ScootCycleTime": int}
    # permitted SCOOT cycle times
    CYCLE_TIMES = [32, 36, 40, 44, 48, 52, 56, 60, 64, 72, 80, 88, 96, 104, 112, 120]

    def __init__(
        self,
        m37_path,
        periods,
        source_type="averaged",
        export_to_csv_path=None,
        signal_emulator=None,
    ):
        """

        :param m37_path: directory to load M37 data from
        :param periods: parent SignalEmulator object
        """
        super().__init__(item_data=[], signal_emulator=signal_emulator)
        assert source_type in ("averaged", "raw", None)
        self.periods = periods
        if source_type is None:
            self.m37_df = pd.DataFrame()
        elif source_type == "raw":
            self.m37_data = self.load_all_m37_in_directory_df(m37_path)
            self.m37_df = self.calculate_average_signal_timings()
            if export_to_csv_path:
                self.export_to_csv(export_to_csv_path)
        elif source_type == "averaged":
            self.m37_df = pd.read_csv(
                m37_path,
                dtype=self.COLUMN_DTYPES,
            )
        self.data = {}
        for index, row in self.m37_df.iterrows():
            m37 = M37Average(
                node_id=row["NodeId"],
                site_number=row["SiteId"],
                stage_id=row["UtcStageId"],
                period_id=row["PeriodId"],
                green_time=row["FinalGreenTime"],
                interstage_time=row["FinalInterstageTime"],
                cycle_time=row["ScootCycleTime"],
                signal_emulator=signal_emulator,
            )
            self.data[m37.get_key()] = m37

    def get_cycle_time_by_site_id_and_period_id(self, site_id, period_id):
        for stage_id in M37StageToStageNumber:
            if self.key_exists((site_id, stage_id.name, period_id)):
                return self.get_by_key((site_id, stage_id.name, period_id)).cycle_time
        else:
            return None

    def calculate_average_signal_timings(self):
        """
        Function to return a DataFrame of averaged M37 signal timings
        :return: DataFrame of M37 timings
        """
        m37_all = pd.DataFrame()
        for period in self.periods:
            # filter M37s to the time bounds of the time Period
            m37_filtered = self.m37_data.between_time(
                start_time=period.start_time_str, end_time=period.end_time_str
            )
            # Group by NodeId, SiteId and UtcStageId
            m37_grouped_node_site_stage = m37_filtered.groupby(["NodeId", "SiteId", "UtcStageId"])
            # aggregate to get the number of Occurrences, average green time and total green times
            m37 = m37_grouped_node_site_stage.agg(
                Occurrences=("Ig", "count"),
                InterstageTotal=("Ig", "sum"),
                InterstageAverage=("Ig", "mean"),
                GreenTotal=("Gn", "sum"),
                GreenAverage=("Gn", "mean"),
            )
            m37["StartTimestamp"] = period.start_time
            m37["EndTimestamp"] = period.end_time
            m37["PeriodId"] = period.name
            m37["StageTotal"] = m37["GreenTotal"] + m37["InterstageTotal"]
            m37_cycles = (
                m37_grouped_node_site_stage.size().groupby(["NodeId", "SiteId"]).agg(Cycles="max")
            )
            m37 = m37.merge(m37_cycles, left_index=True, right_index=True, how="outer")
            total_times = m37.groupby(["NodeId", "SiteId"]).agg(TotalTime=("StageTotal", "sum"))
            total_times["coverage"] = total_times["TotalTime"] / period.total_seconds
            m37 = m37.merge(total_times, left_index=True, right_index=True, how="outer")
            # Calculate the proportion og stage occurrences to the total number of cycles
            m37["OccurrenceFactor"] = m37["Occurrences"] / m37["Cycles"]
            # Apply occurrence factor to the green times and interstage times
            m37["AdjustedGreenAverage"] = m37["GreenAverage"] * m37["OccurrenceFactor"]
            m37["AdjustedInterstageAverage"] = m37["InterstageAverage"] * m37["OccurrenceFactor"]
            m37_cycle_time = m37.groupby(["NodeId", "SiteId"]).agg(
                AdjustedGreenAverageSum=("AdjustedGreenAverage", "sum"),
                AdjustedInterstageAverageSum=("AdjustedInterstageAverage", "sum"),
            )
            # sum the adjusted green times and interstage times to get the cycle time
            m37_cycle_time["CycleTime"] = (
                m37_cycle_time["AdjustedGreenAverageSum"]
                + m37_cycle_time["AdjustedInterstageAverageSum"]
            )
            m37 = m37.merge(
                m37_cycle_time["CycleTime"],
                left_index=True,
                right_index=True,
                how="outer",
            )
            # get the nearest SCOOT cycle time
            m37["ScootCycleTime"] = m37["CycleTime"].apply(
                lambda x: min(self.CYCLE_TIMES, key=lambda y: abs(y - x))
            )
            # factor green times and interstage times to match the SCOOT cycle time
            m37["ScootGreenTime"] = (
                m37["ScootCycleTime"] / m37["CycleTime"] * m37["AdjustedGreenAverage"]
            )
            m37["ScootInterstageTime"] = (
                m37["ScootCycleTime"] / m37["CycleTime"] * m37["AdjustedInterstageAverage"]
            )
            m37_unstack = m37[["ScootGreenTime", "ScootInterstageTime"]].unstack(level=2)
            # apply rounding function that rounds times to whole seconds,
            # while maintaining the total (cycle time)
            m37_unstack = m37_unstack.apply(lambda x: self.rounding_func(x), axis=1)
            m37_stack = m37_unstack.stack()
            m37_stack.rename(
                columns={
                    "ScootGreenTime": "FinalGreenTime",
                    "ScootInterstageTime": "FinalInterstageTime",
                },
                inplace=True,
            )
            m37 = m37.merge(m37_stack, left_index=True, right_index=True, how="outer")
            m37.reset_index(inplace=True)
            m37_all = pd.concat([m37_all, m37], ignore_index=True)
            m37_all["FinalGreenTime"] = m37_all["FinalGreenTime"].astype(int)
            m37_all["FinalInterstageTime"] = m37_all["FinalInterstageTime"].astype(int)
            m37_all["ScootCycleTime"] = m37_all["ScootCycleTime"].astype(int)
        return m37_all

    @staticmethod
    def rounding_func(original_series):
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
        original_total = int(original_series.sum())
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

    @staticmethod
    def load_all_m37_in_directory_df(directory_path):
        """
        Method to load all M37 files found in directory
        :param directory_path: path to directory
        :return: DataFrame of M37 data
        """

        m37_all_df = pd.DataFrame()
        for filename in os.listdir(directory_path):
            file_path = os.path.join(directory_path, filename)
            if filename.endswith("csv") and os.path.isfile(file_path):
                m37_df = pd.read_csv(file_path)
                m37_all_df = pd.concat([m37_all_df, m37_df], ignore_index=True)
        m37_all_df["UtcDateTimestamp"] = pd.to_datetime(m37_all_df["UtcDateTimestamp"])
        m37_all_df.set_index("UtcDateTimestamp", inplace=True)
        return m37_all_df

    def export_to_csv(self, output_path):
        self.m37_df.to_csv(output_path, index=False)


if __name__ == "__main__":
    m37_file = M37Averages(
        m37_path="resources/M37_Timings_May10_AM.csv",
        source_type="raw",
        periods=TimePeriods(
            [
                {"name": "AM", "start_time": "08:00:00", "end_time": "09:00:00"},
                {"name": "OP", "start_time": "10:00:00", "end_time": "16:00:00"},
                {"name": "PM", "start_time": "16:00:00", "end_time": "19:00:00"},
            ]
        ),
    )
