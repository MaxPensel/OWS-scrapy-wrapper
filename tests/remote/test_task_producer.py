import pytest

from remote.task_producer import send_task


def test_send_task(spec_single):
    """Correct crawl execution via message queue."""

    # execute crawl
    send_task(spec_single)
    assert True == False
