import logging

from smart_alerts import SmartAlerts


def test_smart_alerts_logs_exceptions(caplog):
    sa = SmartAlerts()

    def failing_rule(metrics):
        raise RuntimeError("boom")

    sa.add_rule("fail", failing_rule)

    with caplog.at_level(logging.ERROR):
        alerts = sa.evaluate({})

    assert alerts == []
    assert any("fail" in record.getMessage() for record in caplog.records)
