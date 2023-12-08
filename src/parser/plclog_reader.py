import datetime
import io
import zipfile
import numpy as np
import pandas as pd
from .scripts import oledatetime_to_datetime, read_struct_from_binary
import pathlib


def read_plc_log(file: str | zipfile.ZipFile | io.BytesIO,
                 use_timestamp=False,
                 read_series=False):
    """This function will read a PlcLog file and output a dataframe containing all the traces,
    yiu can decide if you want to read all continus files with read_series"""

    if type(file) == zipfile.ZipFile:
        f = file.open(file.filelist[0].filename, "r")
    elif type(file) == str:
        path = pathlib.Path(file)
        print(path)
        if zipfile.is_zipfile(file):
            zfile = zipfile.ZipFile(file)
            f = zfile.open(zfile.filelist[0].filename, "r")
        else:
            f = open(file, "rb")

    sample_cnt = int.from_bytes(f.read(4), "little")
    VersionOfData = int.from_bytes(f.read(4), "little")
    sample_start = oledatetime_to_datetime(np.frombuffer(f.read(8), np.float64))

    if use_timestamp:
        sample_dt = pd.TimedeltaIndex(np.frombuffer(f.read(sample_cnt * 4), dtype=np.float32), unit="s")
    else:
        sample_dt = pd.TimedeltaIndex(np.frombuffer(f.read(sample_cnt * 4), dtype=np.float32), unit="s") + sample_start

    channel_cnt = int.from_bytes(f.read(4), "little")
    prev_fileName = read_struct_from_binary(f)
    next_fileName = read_struct_from_binary(f)

    data = []
    for i in range(0, channel_cnt):
        name = read_struct_from_binary(f)

        new = pd.Series(np.frombuffer(f.read(sample_cnt * 4), dtype=np.float32), sample_dt, name=name)

        data.append(new)

    f.close()

    df = pd.concat(data, axis=1)

    if read_series:
        path = pathlib.Path(file)
        next_path = path.parent.joinpath(next_fileName)
        if next_path.is_file():
            if prev_fileName != next_fileName:
                new = read_plc_log(str(next_path), read_series=True)
                df = pd.concat([df, new])
                df = df[~df.index.duplicated(keep='first')]

    return df
