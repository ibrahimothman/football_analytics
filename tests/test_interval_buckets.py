from src.transforms.gold_intervals import create_interval_label

def test_first_half_interval_label():
    label = create_interval_label(
        period=1,
        interval_start=15,
        is_stoppage_time=False,
    )
    assert label == "15-20"


def test_first_half_stoppage_time_interval_label():
    label = create_interval_label(
        period=1,
        interval_start=50,
        is_stoppage_time=True,
    )
    assert label == "45+"

def test_second_half_interval_label():
    label = create_interval_label(
        period=2,
        interval_start=0,
        is_stoppage_time=False,
    )
    assert label == "45-50"


def test_second_half_stoppage_time_interval_label():
    label = create_interval_label(
        period=2,
        interval_start=100,
        is_stoppage_time=True,
    )
    assert label == "90+"

