from src.metrics.expected_threat import coordinate_to_cell, load_xt_grid, rate_move

def test_coordinate_origin():
    x = 0
    y = 0
    cell = coordinate_to_cell(x, y)
    assert cell == (0, 0)

def test_coordinate_pitch_boundary():
    x = 105
    y = 68
    cell = coordinate_to_cell(x, y)
    assert cell == (11, 7)

def test_xt_added_is_end_minus_start():
    grid = load_xt_grid()
    xt_start, xt_end, xt_added = rate_move(
        grid=grid,
        start_x=10,
        start_y=10,
        end_x=100,
        end_y=60,
    )
    assert xt_added == xt_end - xt_start

def test_forward_move_increases_xt():
    grid = load_xt_grid()
    xt_start, xt_end, xt_added = rate_move(
        grid=grid,
        start_x=10,
        start_y=10,
        end_x=100,
        end_y=60,
    )
    assert xt_added > 0
