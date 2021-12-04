import datetime
import zam.zam

def test_foo():
    assert zam.zam.do_prune(None) == datetime.datetime.max
