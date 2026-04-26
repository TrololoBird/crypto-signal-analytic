# Scripts inventory

Ниже зафиксирован актуальный реестр операторских/диагностических скриптов из `scripts/`.

| script | назначение | статус (active/deprecated) | входные параметры | пример запуска |
|---|---|---|---|---|
| `check_scripts_readme.py` | CI-guard: проверяет, что каждый `scripts/*.py` описан в этой таблице. | active | нет | `python scripts/check_scripts_readme.py` |
| `common.py` | Общие утилиты для CLI-скриптов (например, нормализация `sys.path` до корня репозитория). | active | нет (модуль-утилита) | `python -c "from scripts.common import ensure_repo_root_on_syspath"` |
| `full_indicators_registry.py` | Legacy-аудит полного реестра индикаторов (заменён более узкими live/runtime проверками). | deprecated | `--help` (wrapper) | `python -m scripts.full_indicators_registry` |
| `generate_audit_artifacts.py` | Legacy-генерация audit-артефактов для разового внутреннего анализа. | deprecated | `--help` (wrapper) | `python -m scripts.generate_audit_artifacts` |
| `live_check_binance_api.py` | Live smoke-check REST/WS Binance (коннект, реконы, свежесть потоков). | active | `--symbols`, `--warmup-seconds`, `--reconnect-wait-seconds` | `python -m scripts.live_check_binance_api --symbols BTCUSDT ETHUSDT` |
| `live_check_enrichments.py` | Live-проверка заполненности enrichment-полей в `PreparedSymbol`. | active | `--symbols`, `--warmup`, `--show-values` | `python scripts/live_check_enrichments.py --symbols BTCUSDT ETHUSDT --warmup 30` |
| `live_check_indicators.py` | Live-проверка индикаторов/подготовки фреймов по символам или run-id. | active | `--symbols`, `--run-id`, `--concurrency` | `python scripts/live_check_indicators.py --symbols BTCUSDT ETHUSDT` |
| `live_check_strategies.py` | Live-проверка отработки стратегий и распределения решений. | active | `--symbols`, `--run-id`, `--concurrency` | `python scripts/live_check_strategies.py --symbols BTCUSDT ETHUSDT` |
| `live_runtime_monitor.py` | Live-мониторинг runtime-логов и сбор сводной статистики. | active | `--duration`, `--poll-interval`, `--log-dir` | `python -m scripts.live_runtime_monitor --duration 1800` |
| `live_smoke_bot.py` | Smoke-запуск `SignalBot` с fake broadcaster и проверкой startup-sweep. | active | `--tracking-id`, `--warmup-seconds` | `python scripts/live_smoke_bot.py --tracking-id demo --warmup-seconds 30` |
| `migrate_configs.py` | Legacy one-off мигратор конфигов стратегий. | deprecated | `--help` (wrapper) | `python scripts/migrate_configs.py` |
| `monitor_runtime.py` | Legacy runtime monitor под старый ручной/Windows поток запуска. | deprecated | `--help` (wrapper) | `python scripts/monitor_runtime.py` |
| `runtime_audit.py` | Глубокий runtime-аудит telemetry/DB с аналитикой воронки. | active | `--run-id`, `--db-path`, `--telemetry-dir` | `python scripts/runtime_audit.py --run-id <RUN_ID>` |
| `validate_config.py` | Проверка консистентности `config.toml` и базовых инвариантов подготовки фич/индикаторов. | active | `--config`, `--symbol`, `--interval`, `--limit` | `python scripts/validate_config.py --config config.toml` |

## Политика по статусам

- **active**: используется в текущем операционном цикле.
- **deprecated**: оставлен на один релиз как thin-wrapper с подсказкой на замену.

## Удалённые ad-hoc скрипты

- `quick_check.py` удалён: использовал хардкод пути и run-id.
