

def test_read_plc_log():
    from parser.src.parser import read_plc_log
    df = read_plc_log(r"C:\Users\enese\PycharmProjects\Performance Center\TT_Auto\Plslog at 28.09.2023 13-58-27.zip", read_series=True)
    assert df is not None

