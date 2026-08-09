from src.build_silver import is_progressive_pass, calculate_progression

def test_progressive_pass_above_threshold():
    start_x = 30
    start_y = 50
    end_x = 70
    end_y = 50
    distance_gained, progress_ratio = calculate_progression(start_x, start_y, end_x, end_y)

    assert distance_gained > 0
    assert progress_ratio > 0.25

    assert is_progressive_pass(True, progress_ratio)

def test_backward_pass_not_progressive():
    _, ratio = calculate_progression(
        start_x=70,
        start_y=34,
        end_x=50,
        end_y=34,
    )

    assert ratio < 0

    assert not is_progressive_pass(
        is_completed=True,
        progress_ratio=ratio,
    )    


def test_incomplete_pass_not_progressive():
    assert not is_progressive_pass(
        is_completed=False,
        progress_ratio=0.50,
    )    
    