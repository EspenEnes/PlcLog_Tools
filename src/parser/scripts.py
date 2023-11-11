from datetime import datetime, timedelta
import math
from typing import BinaryIO


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
