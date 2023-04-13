import extrainterpreters

def test_running_plain_func_works():
    from math import cos

    with extrainterpreters.Interpreter() as interp:
        assert interp.run(cos, 0.) == 1.0
