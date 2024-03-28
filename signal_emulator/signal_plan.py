from signal_emulator.controller import BaseCollection, BaseItem, PhaseTiming
from signal_emulator.enums import M37StageToStageNumber
from dataclasses import dataclass


@dataclass(eq=False)
class SignalPlan(BaseItem):
    PROBABLY_ZERO = 0
    signal_emulator: object
    controller_key: str
    signal_plan_number: str
    cycle_time: int
    name: str
    time_period_id: str

    def __post_init__(self):
        self.signal_plan_streams = []
        self.controller.signal_plans.append(self)

    def get_key(self):
        return self.controller_key, self.signal_plan_number

    @property
    def controller(self):
        return self.signal_emulator.controllers.get_by_key(self.controller_key)

    @property
    def time_period(self):
        return self.signal_emulator.time_periods.get_by_key(self.time_period_id)

    def emulate(self):
        self.signal_emulator.visum_signal_controllers.add_visum_signal_controller(
            self.controller_key, self.controller.visum_controller_name, self.cycle_time, self.time_period_id, self.signal_emulator.run_datestamp
        )
        for signal_plan_stream in self.signal_plan_streams:
            self.signal_emulator.logger.info(
                f"Emulating Signal Plan Stream: {signal_plan_stream.site_id}"
            )
            signal_plan_stream.emulate()


class SignalPlans(BaseCollection):
    ITEM_CLASS = SignalPlan
    TABLE_NAME = "signal_plans"
    WRITE_TO_DATABASE = True

    def __init__(self, item_data, signal_emulator):
        super().__init__(item_data=item_data, signal_emulator=signal_emulator)
        self.signal_emulator = signal_emulator

    def add_from_stream_plan_dict(self, streams_and_plans, period, signal_plan_number):
        first_plan = next(iter(streams_and_plans.values()))
        first_stream = next(iter(streams_and_plans.keys()))
        max_cycle_time = self.get_cycle_time(first_stream, first_plan)
        signal_plan = SignalPlan(
            controller_key=first_stream.controller.controller_key,
            signal_emulator=self.signal_emulator,
            signal_plan_number=signal_plan_number,
            cycle_time=max_cycle_time,
            name=first_plan.name,
            time_period_id=period.get_key(),
        )
        self.add_instance(signal_plan)

        for stream, plan in streams_and_plans.items():
            m37_stages = self.get_m37_stage_numbers(stream.site_number)
            stage_sequence = plan.get_stage_sequence(m37_stages=m37_stages, stream=stream, cycle_time=max_cycle_time)
            signal_plan_stream = SignalPlanStream(
                signal_emulator=self.signal_emulator,
                controller_key=stream.controller.controller_key,
                site_id=stream.site_number,
                signal_plan_number=signal_plan_number,
                stream_number=stream.stream_number_linsig,
                first_stage_time=stage_sequence[0].pulse_time,
                cycle_time=self.get_cycle_time(stream, plan),
                single_double_triple=1,
                is_va=False,
            )
            self.signal_emulator.signal_plan_streams.add_instance(signal_plan_stream)

            signal_plan_sequence_number = 0
            for previous_ssi, this_ssi, next_ssi in stage_sequence.iter_previous_current_next():
                total_length = self.get_stage_length_from_pulse_points(
                    this_ssi.pulse_time,
                    next_ssi.pulse_time,
                    max_cycle_time, #plan.cycle_time,
                )
                m37 = this_ssi.stage.get_m37(stream.site_number)
                if m37 and m37.utc_stage_id not in {"PG", "GX"}:
                    interstage_length = m37.interstage_time
                else:
                    interstage_length = signal_plan_stream.get_interstage_time(
                        previous_ssi.stage, this_ssi.stage, modified=False
                    )
                if this_ssi.effective_stage_call_rate < 1:
                    interstage_length = int(interstage_length * this_ssi.effective_stage_call_rate)
                elif previous_ssi.effective_stage_call_rate < 1:
                    interstage_length = int(interstage_length * previous_ssi.effective_stage_call_rate)

                signal_plan_stage = SignalPlanStage(
                    signal_emulator=self.signal_emulator,
                    controller_key=stream.controller.controller_key,
                    signal_plan_number=signal_plan_number,
                    stream_number=stream.stream_number_linsig,
                    signal_plan_sequence_number=signal_plan_sequence_number,
                    site_id=stream.site_number,
                    stage_number=this_ssi.stage.stage_number,
                    total_length=total_length,
                    interstage_length=interstage_length,
                    green_length=total_length - interstage_length,
                    pulse_point=this_ssi.pulse_time,
                    either_or=False,
                    fixed_length=False,
                )
                self.signal_emulator.signal_plan_stages.add_instance(signal_plan_stage)
                signal_plan_sequence_number += 1

    def get_cycle_time(self, stream, plan):
        cycle_time = self.get_m37_cycle_time(stream)
        if cycle_time:
            return cycle_time
        else:
            return self.get_plan_cycle_time(plan)

    def get_m37_cycle_time(self, stream):
        for stage in M37StageToStageNumber:
            if self.signal_emulator.m37s.key_exists((
                stream.site_number, stage.value, self.signal_emulator.time_periods.active_period_id
            )):
                return self.signal_emulator.m37s.get_by_key((
                stream.site_number, stage.value, self.signal_emulator.time_periods.active_period_id
            )).cycle_time
        if stream.site_number != stream.controller_key:
            return self.get_m37_cycle_time(self.signal_emulator.streams.get_by_key((stream.controller_key, 0)))
        else:
            return None

    @staticmethod
    def get_plan_cycle_time(plan):
        return plan.cycle_time

    @staticmethod
    def get_stage_length_from_pulse_points(pulse_point_1, pulse_point_2, cycle_time):
        return (pulse_point_2 - pulse_point_1 + cycle_time) % cycle_time

    def get_m37_stage_numbers(self, site_number):
        m37_stages_numbers = set()
        for (m37_bit, stage_number) in M37StageToStageNumber.__members__.items():
            if (
                self.signal_emulator.m37s.key_exists(
                    (
                        site_number,
                        stage_number.value,
                        self.signal_emulator.time_periods.active_period_id,
                    )
                )
                and self.signal_emulator.m37s.get_by_key(
                    (
                        site_number,
                        stage_number.value,
                        self.signal_emulator.time_periods.active_period_id,
                    )
                ).total_time
                > 0
            ):
                m37_stages_numbers.add(stage_number.value)
            elif (
                self.signal_emulator.m37s.key_exists(
                    (
                        site_number.replace("J", "P"),
                        m37_bit,
                        self.signal_emulator.time_periods.active_period_id,
                    )
                )
                and self.signal_emulator.m37s.get_by_key(
                    (
                        site_number.replace("J", "P"),
                        m37_bit,
                        self.signal_emulator.time_periods.active_period_id,
                    )
                ).total_time
                > 0
            ):
                m37_stages_numbers.add(stage_number.value)
        return m37_stages_numbers


@dataclass(eq=False)
class SignalPlanStream(BaseItem):
    signal_emulator: object
    controller_key: str
    site_id: str
    signal_plan_number: str
    stream_number: int
    first_stage_time: int
    cycle_time: int
    single_double_triple: int
    is_va: bool
    PROBABLY_ZERO = 0

    def __post_init__(self):
        self.signal_plan.signal_plan_streams.append(self)
        self.signal_plan_stages = []

    def get_key(self):
        return self.controller_key, self.signal_plan_number, self.stream_number

    def get_signal_plan_key(self):
        return self.controller_key, self.signal_plan_number

    @property
    def site_number_int(self):
        parts = self.site_id.split("/")
        if parts[0][0].isalpha():
            parts[0] = parts[0][1:]
        return int(parts[0]) * 1000 + int(parts[1])

    @property
    def signal_plan(self):
        return self.signal_emulator.signal_plans.get_by_key(self.get_signal_plan_key())

    @property
    def controller(self):
        return self.signal_emulator.controllers.get_by_key(self.controller_key)

    @property
    def stream(self):
        return self.signal_emulator.streams.get_by_key(
            (self.controller_key, self.stream_number_controller)
        )

    @property
    def stream_number_controller(self):
        return self.stream_number - 1

    def emulate(self):
        stream = self.stream
        self.signal_emulator.time_periods.active_period_id = self.signal_plan.time_period_id
        m37_stages = self.signal_emulator.signal_plans.get_m37_stage_numbers(stream.site_number)
        m37_check = len(m37_stages) > 0
        if m37_check:
            cycle_time = self.signal_emulator.m37s.get_cycle_time_by_site_id_and_period_id(
                stream.site_number, self.signal_emulator.time_periods.active_period_id
            )
            self.signal_emulator.logger.info("M37s used for stage lengths")
        else:
            cycle_time = self.cycle_time
            self.signal_emulator.logger.info(
                "M37s not found, plan pulse times used for stage lengths"
            )

        # self.signal_emulator.visum_signal_controllers.add_visum_signal_controller(
        #     self.controller_key, self.controller.address, cycle_time, self.signal_plan.time_period_id, self.signal_emulator.run_datestamp
        # )

        stream.active_stage_key = stream.controller_key, self.signal_plan_stages[-1].stage_number
        if len(self.signal_plan_stages) == 1:
            for phase in self.signal_plan_stages[-1].stage.phases_in_stage:
                phase_timing = PhaseTiming(
                    signal_emulator=self.signal_emulator,
                    controller_key=stream.controller_key,
                    site_id=stream.site_number,
                    phase_ref=phase.phase_ref,
                    index=len(phase.phase_timings),
                    start_time=0,
                    end_time=self.cycle_time,
                    time_period_id=self.signal_plan.time_period_id,
                )
                self.signal_emulator.phase_timings.add_instance(phase_timing)

        all_phases_used = {
            phase for sps in self.signal_plan_stages for phase in sps.stage.phases_in_stage
        }
        for index, signal_plan_stage in enumerate(
            self.signal_plan_stages + [self.signal_plan_stages[0]]
        ):
            current_stage = stream.active_stage
            m37 = self.signal_emulator.m37s.get_by_key(
                (
                    self.site_id,
                    signal_plan_stage.stage.stream_stage_number,
                    self.signal_emulator.time_periods.active_period_id,
                )
            )
            if not m37:
                m37 = self.signal_emulator.m37s.get_by_key(
                    (
                        self.site_id,
                        signal_plan_stage.stage.stream_stage_number,
                        self.signal_emulator.time_periods.active_period_id,
                    )
                )

            end_phases = self.signal_emulator.stages.get_end_phases(
                current_stage, signal_plan_stage.stage
            )
            start_phases = self.signal_emulator.stages.get_start_phases(
                current_stage, signal_plan_stage.stage
            )
            controller_interstage_time = self.get_interstage_time(
                current_stage,
                signal_plan_stage.stage,
            )

            if controller_interstage_time < signal_plan_stage.interstage_length:
                print(stream.controller_key, controller_interstage_time, signal_plan_stage.interstage_length)

            if controller_interstage_time > signal_plan_stage.interstage_length:
                self.signal_emulator.logger.info(
                    f"Controller interstage time: {controller_interstage_time} greater than SignalPlanStage "
                    f"interstage time: {signal_plan_stage.interstage_length}, so controller intergreens are adjusted"
                )
                self.reduce_interstage(
                    controller_key=stream.controller_key,
                    end_stage_key=current_stage.stage_number,
                    start_stage_key=signal_plan_stage.stage_number,
                    interstage_time=signal_plan_stage.interstage_length,
                )

            if not index == 0:
                for end_phase in end_phases:
                    end_time = None
                    if (
                        end_phase.associated_phase
                        and end_phase.termination_type.name == "ASSOCIATED_PHASE_GAINS_ROW"
                    ):
                        max_start_time_delta = self.get_max_start_time(
                            end_phases=end_phases,
                            start_phase=end_phase.associated_phase,
                            end_stage_key=current_stage.stage_number,
                            start_stage_key=signal_plan_stage.stage_number,
                        )
                        end_time = self.constrain_time_to_cycle_time(
                            signal_plan_stage.pulse_point + max_start_time_delta, cycle_time
                        )
                    elif end_phase.termination_type.name == "END_OF_STAGE":
                        end_time = self.constrain_time_to_cycle_time(
                            signal_plan_stage.pulse_point
                            + self.signal_emulator.phase_delays.get_delay_time_by_stage_and_phase_keys(
                                controller_key=stream.controller_key,
                                end_stage_key=current_stage.stage_number,
                                start_stage_key=signal_plan_stage.stage_number,
                                phase_key=end_phase.phase_ref,
                            ),
                            cycle_time,
                        )
                        if end_phase.indicative_arrow_phase:
                            if end_phase.indicative_arrow_phase.phase_timings:
                                if (
                                    end_phase.indicative_arrow_phase.phase_timings[-1].end_time
                                    is None
                                ):
                                    end_phase.indicative_arrow_phase.phase_timings[
                                        -1
                                    ].end_time = end_time
                            elif end_phase.indicative_arrow_phase in all_phases_used:
                                pass
                                phase_timing = PhaseTiming(
                                    signal_emulator=self.signal_emulator,
                                    controller_key=stream.controller_key,
                                    site_id=stream.site_number,
                                    phase_ref=end_phase.indicative_arrow_phase.phase_ref,
                                    index=len(end_phase.indicative_arrow_phase.phase_timings),
                                    end_time=end_time,
                                    time_period_id=self.signal_plan.time_period_id,
                                )
                                self.signal_emulator.phase_timings.add_instance(phase_timing)
                    if end_time is not None:
                        if len(end_phase.phase_timings) > 0:
                            last_phase_timing = end_phase.phase_timings[-1]
                        else:
                            last_phase_timing = None

                        if last_phase_timing and last_phase_timing.end_time is None:
                            last_phase_timing.end_time = end_time
                        else:
                            phase_timing = PhaseTiming(
                                signal_emulator=self.signal_emulator,
                                controller_key=stream.controller_key,
                                site_id=stream.site_number,
                                phase_ref=end_phase.phase_ref,
                                index=len(end_phase.phase_timings),
                                end_time=end_time,
                                time_period_id=self.signal_plan.time_period_id,
                            )
                            self.signal_emulator.phase_timings.add_instance(phase_timing)
            if not index == len(self.signal_plan_stages):
                for start_phase in start_phases:
                    max_start_time_delta = self.get_max_start_time(
                        end_phases=end_phases,
                        start_phase=start_phase,
                        end_stage_key=current_stage.stage_number,
                        start_stage_key=signal_plan_stage.stage_number,
                    )
                    start_time = self.constrain_time_to_cycle_time(
                        signal_plan_stage.pulse_point + max_start_time_delta, cycle_time
                    )
                    if len(start_phase.phase_timings) > 0:
                        last_phase_timing = start_phase.phase_timings[-1]
                    else:
                        last_phase_timing = None

                    if last_phase_timing and last_phase_timing.start_time is None:
                        last_phase_timing.start_time = start_time
                    else:
                        phase_timing = PhaseTiming(
                            signal_emulator=self.signal_emulator,
                            controller_key=stream.controller_key,
                            site_id=stream.site_number,
                            phase_ref=start_phase.phase_ref,
                            index=len(start_phase.phase_timings),
                            start_time=start_time,
                            time_period_id=self.signal_plan.time_period_id,
                        )
                        self.signal_emulator.phase_timings.add_instance(phase_timing)

            stream.active_stage_key = (stream.controller_key, signal_plan_stage.stage_number)

    @staticmethod
    def constrain_time_to_cycle_time(time, cycle_time):
        return time % cycle_time

    def reduce_interstage(self, controller_key, end_stage_key, start_stage_key, interstage_time):
        end_stage = self.signal_emulator.stages.get_by_key((controller_key, end_stage_key))
        start_stage = self.signal_emulator.stages.get_by_key((controller_key, start_stage_key))
        end_phases = self.signal_emulator.stages.get_end_phases(end_stage, start_stage)
        start_phases = self.signal_emulator.stages.get_start_phases(end_stage, start_stage)
        original_interstage = self.get_interstage_time(end_stage, start_stage)
        for start_phase in start_phases:
            for end_phase in end_phases:
                end_phase_delay = self.signal_emulator.phase_delays.get_by_key(
                    (controller_key, end_stage_key, start_stage_key, end_phase.phase_ref),
                    modified=True,
                )
                intergreen = self.signal_emulator.intergreens.get_by_key(
                    (controller_key, end_phase.phase_ref, start_phase.phase_ref), modified=True
                )
                start_phase_delay = self.signal_emulator.phase_delays.get_by_key(
                    (
                        controller_key,
                        end_stage_key,
                        start_stage_key,
                        start_phase.phase_ref,
                    ),
                    modified=True,
                )
                start_phase_delay_time = start_phase_delay.delay_time
                end_phase_delay_time = end_phase_delay.delay_time
                intergreen_time = intergreen.intergreen_time
                if end_phase_delay_time > interstage_time:
                    self.signal_emulator.modified_phase_delays.add_item(
                        {
                            "controller_key": end_phase_delay.controller_key,
                            "end_stage_key": end_phase_delay.end_stage_key,
                            "start_stage_key": end_phase_delay.start_stage_key,
                            "phase_ref": end_phase_delay.phase_ref,
                            "time_period_id": self.signal_emulator.time_periods.active_period_id,
                            "delay_time": interstage_time,
                            "original_delay_time": end_phase_delay.delay_time,
                            "is_absolute": True,
                        }
                    )
                    end_phase_delay_time = interstage_time
                if end_phase_delay_time + intergreen_time > interstage_time:
                    self.signal_emulator.modified_intergreens.add_item(
                        {
                            "controller_key": intergreen.controller_key,
                            "end_phase_key": intergreen.end_phase_key,
                            "start_phase_key": intergreen.start_phase_key,
                            "time_period_id": self.signal_emulator.time_periods.active_period_id,
                            "intergreen_time": interstage_time - end_phase_delay_time,
                            "original_time": intergreen.intergreen_time,
                        },
                        signal_emulator=self.signal_emulator,
                    )
                if start_phase_delay_time > interstage_time:
                    self.signal_emulator.modified_phase_delays.add_item(
                        {
                            "controller_key": start_phase_delay.controller_key,
                            "end_stage_key": start_phase_delay.end_stage_key,
                            "start_stage_key": start_phase_delay.start_stage_key,
                            "phase_ref": start_phase_delay.phase_ref,
                            "time_period_id": self.signal_emulator.time_periods.active_period_id,
                            "delay_time": interstage_time,
                            "original_delay_time": start_phase_delay.delay_time,
                            "is_absolute": True,
                        }
                    )

        reduced_interstage = self.get_interstage_time(end_stage, start_stage)
        self.signal_emulator.logger.info(
            f"interstage_time: {original_interstage}, reduced interstage: {reduced_interstage}"
        )
        assert interstage_time == reduced_interstage

    def get_interstage_time(self, end_stage, start_stage, modified=True):
        end_phases = self.signal_emulator.stages.get_end_phases(end_stage, start_stage)
        start_phases = self.signal_emulator.stages.get_start_phases(end_stage, start_stage)
        max_interstage_time = 0
        for start_phase in start_phases:
            interstage_time = self.get_max_start_time(
                end_phases, start_phase, end_stage.stage_number, start_stage.stage_number, modified
            )
            max_interstage_time = max(max_interstage_time, interstage_time)
        return max_interstage_time

    def get_max_start_time(self, end_phases, start_phase, end_stage_key, start_stage_key, modified=True):
        time_delta = 0
        for end_phase in end_phases:
            end_phase_delay = (
                self.signal_emulator.phase_delays.get_delay_time_by_stage_and_phase_keys(
                    controller_key=end_phase.controller_key,
                    end_stage_key=end_stage_key,
                    start_stage_key=start_stage_key,
                    phase_key=end_phase.phase_ref,
                    modified=modified,
                )
            )
            intergreen = self.signal_emulator.intergreens.get_intergreen_time_by_phase_keys(
                controller_key=end_phase.controller_key,
                end_phase_key=end_phase.phase_ref,
                start_phase_key=start_phase.phase_ref,
                modified=modified,
            )
            start_phase_delay = (
                self.signal_emulator.phase_delays.get_delay_time_by_stage_and_phase_keys(
                    controller_key=end_phase.controller_key,
                    end_stage_key=end_stage_key,
                    start_stage_key=start_stage_key,
                    phase_key=start_phase.phase_ref,
                    modified=modified,
                )
            )
            time_delta = max(time_delta, max(end_phase_delay + intergreen, start_phase_delay))
        return time_delta


class SignalPlanStreams(BaseCollection):
    ITEM_CLASS = SignalPlanStream
    TABLE_NAME = "signal_plan_streams"
    WRITE_TO_DATABASE = True

    def __init__(self, item_data, signal_emulator):
        super().__init__(item_data=item_data, signal_emulator=signal_emulator)
        self.signal_emulator = signal_emulator


@dataclass(eq=False)
class SignalPlanStage(BaseItem):
    signal_emulator: object
    controller_key: str
    signal_plan_number: int
    stream_number: int
    site_id: str
    signal_plan_sequence_number: int
    stage_number: int
    total_length: int
    interstage_length: int
    green_length: int
    pulse_point: int
    either_or: int
    fixed_length: bool

    def __post_init__(self):
        signal_plan_stream = self.signal_emulator.signal_plan_streams.get_by_key(
            (self.controller_key, self.signal_plan_number, self.stream_number)
        )
        signal_plan_stream.signal_plan_stages.append(self)

    def __repr__(self):
        return f"SignalPlanStage: {self.stage_number=} {self.pulse_point=}"

    def get_key(self):
        return (
            self.controller_key,
            self.signal_plan_number,
            self.stage_number,
            self.signal_plan_sequence_number,
        )

    def get_stage_key(self):
        return self.controller_key, self.stage_number

    @property
    def stage(self):
        return self.signal_emulator.stages.get_by_key(self.get_stage_key())


class SignalPlanStages(BaseCollection):
    ITEM_CLASS = SignalPlanStage
    TABLE_NAME = "signal_plan_stages"
    WRITE_TO_DATABASE = True

    def __init__(self, item_data, signal_emulator):
        super().__init__(item_data=item_data, signal_emulator=signal_emulator)
        self.signal_emulator = signal_emulator
