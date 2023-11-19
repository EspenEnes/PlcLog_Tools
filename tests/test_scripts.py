import datetime


def test_oledatetime_to_datetime():
    from parser.src.parser.scripts import oledatetime_to_datetime
    dtime = oledatetime_to_datetime(45000.0)
    assert datetime.datetime(2023,3,15) == dtime


def test_datetime_to_oledatetime():
    from parser.src.parser.scripts import datetime_to_oledatetime
    ole = datetime_to_oledatetime(datetime.datetime(2023,3,15))
    assert ole == 45000.0
