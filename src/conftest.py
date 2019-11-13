"""Configuration for pytest"""
import pytest
import os
import json


def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests")
    parser.addoption(
        "--inttest", action="store_true", default=False, help="run integration tests")
    parser.addoption(
        "--systest", action="store_true", default=False, help="run system tests")


def pytest_collection_modifyitems(config, items):
    # --runslow given in cli: do not skip slow tests
    if config.getoption("--runslow") == False:
        skip_slow = pytest.mark.skip(reason="need --runslow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    # --inttest given in cli: do not skip integration tests
    if config.getoption("--inttest") == False:
        skip_inttest = pytest.mark.skip(reason="need --inttest option to run")
        for item in items:
            if "inttest" in item.keywords:
                item.add_marker(skip_inttest)

    # --systest given in cli: do not skip system tests
    if config.getoption("--systest") == False:
        skip_slow = pytest.mark.skip(reason="need --systest option to run")
        for item in items:
            if "systest" in item.keywords:
                item.add_marker(skip_slow)


"""All required fixtures."""



@pytest.fixture(scope='module')
def test_data_path():
    """Provide path to test data."""
    return 'tests/data'


@pytest.fixture(scope='module')
def spec_single(test_data_path):
    """Provide specification data of simple crawl."""
    # read file
    spec_file = os.path.join(os.getcwd(), test_data_path, 'spec_single.json')
    with open(spec_file) as json_data:
        spec_data = json.load(json_data)
    return spec_data


# @pytest.fixture(scope='module')
# def crawler_result_simple(test_data_path):
#     """Provide result data of simple crawl."""
#     # read file
#     task_file = os.path.join(os.getcwd(), test_data_path, 'zdt', 'task_zdt1_emo.json')
#     with open(task_file) as json_data:
#         task_data = json.load(json_data)
#     return task_data
