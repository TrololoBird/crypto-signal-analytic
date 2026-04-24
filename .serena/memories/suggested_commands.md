Windows/PowerShell commands commonly useful in this repo:
- `rg -n pattern bot` to search code
- `rg --files bot` to list files
- `python -m py_compile <files>` for fast syntax verification
- `python -m compileall bot` for broader compile verification
- `ruff check <path>` for lint checks
- `@'...python...'@ | python -` for inline smoke scripts
- `Get-Content <file> | Select-Object -First/Skip ...` for targeted reads
- `python -m bot.cli` or CLI entrypoints defined in `cli.py` for runtime flows (inspect env/config first)
- `python -m bot.startup_reporter` style invocations via inline script if you need a startup report without Telegram sending.