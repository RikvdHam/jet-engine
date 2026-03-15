from pathlib import Path

TEST_DATA = Path(__file__).parent / "data"


def get_test_file(name):
    return TEST_DATA / name
