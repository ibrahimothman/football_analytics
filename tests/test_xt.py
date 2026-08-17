from src.metrics.expected_threat import continuous_to_cell, rate_move

PITCH_SIZE = (105, 68)
XT_GRID_SIZE = (12, 8)


def create_mock_xt_grid():
    """create a mock xt grid"""
    return [
        [
            column * 0.01 for column in range(XT_GRID_SIZE[0])
        ] for row in range(XT_GRID_SIZE[1])
    ]

def test_coordinate_origin():
    x = 0
    y = 0
    cell = continuous_to_cell(point=(x, y), continuous_size=PITCH_SIZE, discrete_size=XT_GRID_SIZE)
    assert cell == (0, 0)

def test_coordinate_pitch_boundary():
    x = 105
    y = 68
    cell = continuous_to_cell(point=(x, y), continuous_size=PITCH_SIZE, discrete_size=XT_GRID_SIZE)
    assert cell == (11, 7)

def test_xt_added_is_end_minus_start():
    grid = create_mock_xt_grid()
    xt_start, xt_end, xt_added = rate_move(
        grid=grid,
        pitch_size=PITCH_SIZE,
        start_x=10,
        start_y=10,
        end_x=100,
        end_y=60,
    )
    assert xt_added == xt_end - xt_start

def test_forward_move_increases_xt():
    grid = create_mock_xt_grid()
    xt_start, xt_end, xt_added = rate_move(
        grid=grid,
        pitch_size=PITCH_SIZE,
        start_x=10,
        start_y=10,
        end_x=100,
        end_y=60,
    )
    assert xt_added > 0
