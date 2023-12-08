import zipfile

import pandas as pd
from .constants import *
from .scripts import convert_df_to_timeseries


def mmc_step_task(machine_name, df: pd.Series, seq_name, start_dt, end_dt):
    output = []
    start = None
    end = None
    step = None

    mask = (df.index >= start_dt) & (df.index <= end_dt)
    new_df = df.where(mask).dropna()

    new_df = new_df.where(new_df.shift() != new_df).dropna()

    old = None

    for (dt, value) in new_df.items():

        if old != value:
            has_changes = True
        else:
            has_changes = False

        if not start and has_changes:
            if value > 0:
                """ If new task and no task started, start new task"""
                start = dt
                end = None
                step = value
        elif start and has_changes:
            """If new task and a task is already started, end task """
            end = dt
            output.append(
                dict(seq=seq_name, step=step, start=start, end=end, width=0.1 + (step / 100), group=machine_name))

            if value > 0:
                """If task was ended because a new task was forced in, start new task"""
                start = dt
                step = value
                end = None
            elif value == 0:
                start = None
                step = None
                end = None
        old = value

    if start and not end:
        """If task is not ended on last iteration of the data, end task"""
        end = new_df.index[-1]
        output.append(dict(seq=seq_name, step=step, start=start, end=end, width=0.1 + (step / 100), group=machine_name))

    return output


def mmc_tasks(machine_id, seq_pair):
    """Slot Sequense_ID, and Slot Step_ID as inputs
    This function generates tasks from the 5 different slots that MMC can use pr machine,
    each. each task has different active steps that is stored in another DB"""
    output = []
    start = None
    end = None
    task = None

    """Select correct sequense Enum according to selected machine"""
    try:
        match machine_id:
            case Machine_ID.TDDW.value:
                seq_enum = TDDW
            case Machine_ID.PriHR.value:
                seq_enum = HR
            case Machine_ID.HT.value:
                seq_enum = HT
            case _:
                seq_enum = None
    except NameError:
        seq_enum = None

    try:
        machine_name = Machine_ID(machine_id).name
    except:
        machine_name = f"Tool[{int(machine_id)}]"

    # loop over seq_id, seq_step pairs
    for df_seq, df_steps in seq_pair:
        old = None

        for (dt, value) in df_seq.dropna().items():
            if old != value:
                has_changes = True
            else:
                has_changes = False

            if not start and has_changes:
                if value > 0:
                    """ If new task and no task started, start new task"""
                    start = dt
                    end = None
                    task = value
            elif start and has_changes:
                """If new task and a task is already started, end task """
                end = dt
                if seq_enum:
                    try:
                        seq_name = seq_enum(int(task)).name
                    except ValueError:
                        seq_name = f"{machine_name}:Seq[{int(task)}]"
                else:
                    seq_name = f"{machine_name}:Seq[{int(task)}]"

                tasks = mmc_step_task(machine_name, df_steps, seq_name, start, end)
                output.extend(tasks)

                if value > 0:
                    """If task was ended because a new task was forced in, start new task"""
                    start = dt
                    task = value
                    end = None
                elif value == 0:
                    start = None
                    task = None
                    end = None
            old = value

        if start and not end:
            """If task is not ended on last iteration of the data, end task"""
            end = df_seq.index[-1]

            if seq_enum:
                try:
                    seq_name = seq_enum(int(task)).name
                except ValueError:
                    seq_name = f"{machine_name}:Seq[{int(task)}]"
            else:
                seq_name = f"{machine_name}:Seq[{int(task)}]"

            tasks = mmc_step_task(machine_name, df_steps, seq_name, start, end)
            output.extend(tasks)

    return pd.DataFrame(output)


def mmc_seq_tasks(data_frame: pd.DataFrame | zipfile.ZipFile,
                  seq_columns: list[int] | list[list[int]],
                  step_columns: list[int] | list[list[int]],
                  tool_columns: list[int] | None = None,
                  ids: list[int] | None = None
                  ):
    """This function works ass the topfunction, a dataframe or PLCLog is needed, the swq_columns tells us what columns
    in the dataframe contains the Seq_ID, and is paired up with a column containing the Steps. Each pair of data is then
    analysed and gives us a Task with SeqID from Sequense columns and ActiveStep from the Step_column.
    """


    ### verify group identifyers
    if ids and not tool_columns:
        groups = ids
    elif tool_columns and not ids:
        groups = [int(x) for x in data_frame.iloc[:, tool_columns].dropna().iloc[0].values]
    else:
        raise Exception("Only one way to define groups can be used.")

    ### validate

    if type(seq_columns[0]) == list:
        if (len(seq_columns) == len(groups) and (len(step_columns) == len(groups))):
            output = []
            for seq_columns2, step_columns2, machine_id in zip(seq_columns, step_columns, groups):
                seq_pair = []
                seq_column_data = data_frame.iloc[:, seq_columns2]
                seq_column_data = convert_df_to_timeseries(seq_column_data)
                step_column_data = data_frame.iloc[:, step_columns2]
                for seq, step in zip(seq_column_data, step_column_data):
                    new = [seq_column_data[seq], step_column_data[step]]
                    seq_pair.append(new)
                output.append(mmc_tasks(machine_id, seq_pair))
            return pd.concat(output)


        else:
            raise Exception("Length of seq_col, seq_step do not match with length of groups")
    elif type(seq_columns[0]) == int:
        if len(groups) == 1:
            seq_pair = []
            seq_column_data = data_frame.iloc[:, seq_columns]
            step_column_data = data_frame.iloc[:, step_columns]
            for seq, step in zip(seq_column_data, step_column_data):
                new = [seq_column_data[seq], step_column_data[step]]
                seq_pair.append(new)

            machine_id = groups[0]
            return mmc_tasks(machine_id, seq_pair)
        else:
            raise Exception("Length of seq_col, seq_step do not match with length of groups")
