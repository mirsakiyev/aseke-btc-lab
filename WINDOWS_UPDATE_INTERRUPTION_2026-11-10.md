# ASEKE BTC Lab Windows Update interruption

Two planned Windows restarts interrupted collection and analysis. Both collectors and the dashboard recovered. The subsequent KuCoin read supplied the trade and mark minutes spanning the interruption. Earlier missing observations and unknown decisions were accounted for separately from the restart window.

Before a planned restart, record scheduler ownership and current collector cursors. After recovery, verify that each scheduled task owns one worker, collector receipts are advancing, missing market intervals are explicit, and health transitions are deduplicated. Keep the workstation timeline, event-log references, and notification delivery identifiers in the private incident record. [Operational monitoring runbook](HEALTH_MONITORING_RUNBOOK.md)
