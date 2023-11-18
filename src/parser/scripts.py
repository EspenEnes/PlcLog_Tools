import base64
import io
import zipfile
from datetime import datetime, timedelta
import math
from typing import BinaryIO

import pandas as pd


def oledatetime_to_datetime(dateval):
    basedate = datetime(year=1899, month=12, day=30, hour=0, minute=0)
    parts = math.modf(dateval)
    days = timedelta(parts[1])
    day_frac = timedelta(abs(parts[0]))
    return basedate + days + day_frac

def read_struct_from_binary(binary: BinaryIO):
    length = int.from_bytes(binary.read(4), "little")
    bufferd = binary.read(length)
    struct = bufferd.decode("ANSI")
    return struct

def convert_df_to_timeseries(df: pd.DataFrame):
    output = {}
    for col in df:
        new = df[col].where(
            df[col].shift() != df[
                col]).dropna()
        output[col] = new
    return pd.DataFrame(output)

def zipfile_from_bytes(content_bytes):
    content_decoded = base64.b64decode(content_bytes)
    # Use BytesIO to handle the decoded content
    zip_str = io.BytesIO(content_decoded)
    # Now you can use ZipFile to take the BytesIO output
    zfile = zipfile.ZipFile(zip_str, 'r')
    return zfile



        

