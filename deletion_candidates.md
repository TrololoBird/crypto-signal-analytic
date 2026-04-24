# Deletion Candidates

Quarantine-first only. No file listed here was removed in this wave.

## bot/core/analyzer/__init__.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/core/__init__.py, bot/tasks/reporter.py, bot/tasks/tracker.py
- inbound refs: bot/core/__init__.py, bot/tasks/reporter.py, bot/tasks/tracker.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/core/analyzer/metrics.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/core/analyzer/__init__.py, bot/core/analyzer/reporter.py, bot/core/diagnostics/health.py
- inbound refs: bot/core/analyzer/__init__.py, bot/core/analyzer/reporter.py, bot/core/diagnostics/health.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/core/analyzer/reporter.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/core/analyzer/__init__.py
- inbound refs: bot/core/analyzer/__init__.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/core/analyzer/tracker.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/core/analyzer/__init__.py
- inbound refs: bot/core/analyzer/__init__.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/core/diagnostics/__init__.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/core/__init__.py, bot/tasks/reporter.py, bot/tasks/scanner.py, bot/tasks/tracker.py, bot/telegram/sender.py
- inbound refs: bot/core/__init__.py, bot/tasks/reporter.py, bot/tasks/scanner.py, bot/tasks/tracker.py, bot/telegram/sender.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/core/diagnostics/alerts.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/core/diagnostics/__init__.py
- inbound refs: bot/core/diagnostics/__init__.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/core/diagnostics/health.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/core/diagnostics/__init__.py, bot/core/diagnostics/alerts.py
- inbound refs: bot/core/diagnostics/__init__.py, bot/core/diagnostics/alerts.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/core/diagnostics/metrics.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/core/diagnostics/__init__.py
- inbound refs: bot/core/diagnostics/__init__.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/core/self_learner.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py
- inbound refs: none
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/tasks/__init__.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py
- inbound refs: none
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/tasks/reporter.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/tasks/__init__.py
- inbound refs: bot/tasks/__init__.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/tasks/scanner.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/tasks/__init__.py
- inbound refs: bot/tasks/__init__.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/tasks/scheduler.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/tasks/__init__.py
- inbound refs: bot/tasks/__init__.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/tasks/tracker.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/tasks/__init__.py
- inbound refs: bot/tasks/__init__.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/telegram/__init__.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py
- inbound refs: none
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/telegram/queue.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/telegram/__init__.py, bot/telegram/sender.py
- inbound refs: bot/telegram/__init__.py, bot/telegram/sender.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/telegram/sender.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py; inbound refs only from bot/telegram/__init__.py
- inbound refs: bot/telegram/__init__.py
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.

## bot/telegram_bot.py
- status: scaffold
- reachability: none
- evidence: no static path from runtime roots, tests/, or scripts/live_*.py
- inbound refs: none
- action: Quarantine first; no deletion until package exports and external consumers are reviewed.
