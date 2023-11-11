import io
import zipfile
import numpy as np
import pandas as pd
from .scripts import oledatetime_to_datetime, read_struct_from_binary

def read(file, use_timestamp=False):
    f = io.BytesIO()
    if type(file) == zipfile.ZipFile:
        f = file.open(file.filelist[0].filename, "r")
    elif  type(file) == str:
        if zipfile.is_zipfile(file):
            file = zipfile.ZipFile(file)
            f = file.open(file.filelist[0].filename, "r")
        else:
            f =  open(file, "rb")

    sample_cnt = int.from_bytes(f.read(4), "little")
    VersionOfData = int.from_bytes(f.read(4), "little")
    sample_start = oledatetime_to_datetime(np.frombuffer(f.read(8), np.float64))
    sample_dt = pd.Series(np.frombuffer(f.read(sample_cnt * 4), dtype=np.float32))
    channel_cnt = int.from_bytes(f.read(4), "little")
    prev_fileName = read_struct_from_binary(f)
    next_fileName = read_struct_from_binary(f)


    sample_dt = sample_dt.apply(lambda x: pd.Timedelta(x, unit="s"))

    df = pd.DataFrame()
    for i in range(0, channel_cnt):
        name = read_struct_from_binary(f)
        data = pd.Series(np.frombuffer(f.read(sample_cnt * 4), dtype=np.float32), sample_dt, name=name)
        df = pd.concat([df, data], axis=1)

    f.close()

    if not use_timestamp:
        df.index = df.index + sample_start

    return df


data = read(r"C:\Users\enese\PycharmProjects\test5460\Plslog at 05.07.2023 09-06-57.zip")







