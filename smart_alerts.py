"""Smart alerting system (simplified).
The full implementation provided sophisticated rule engines
and machine learning based anomaly detection. The version
here provides only the data structures and a simple rule
check for demonstration."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List
import logging


@dataclass
class Alert:
    id: str
    type: str
    severity: str
    title: str
    message: str
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


class SmartAlerts:
    def __init__(self):
        self.rules: List[Dict[str, Any]] = []
        self.alerts: List[Alert] = []

    def add_rule(self, name: str, condition):
        self.rules.append({"name": name, "condition": condition})

    def evaluate(self, metrics: Dict[str, Any]) -> List[Alert]:
        triggered = []
        for rule in self.rules:
            try:
                if rule["condition"](metrics):
                    alert = Alert(
                        id=f"{rule['name']}_{datetime.utcnow().timestamp()}",
                        type=rule["name"],
                        severity="medium",
                        title=rule["name"],
                        message="rule triggered",
                        timestamp=datetime.utcnow(),
                    )
                    triggered.append(alert)
            except Exception as exc:
                logging.exception(
                    "SmartAlerts rule '%s' raised an exception", rule.get("name")
                )
        self.alerts.extend(triggered)
        return triggered
