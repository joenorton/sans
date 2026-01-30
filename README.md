# sans

Small, deterministic compiler for a strict SAS-like batch subset. It produces a plan and a refusal report. It does not execute data.

## Quick start (check only)
- Install: `python -m pip install -e .`
- Run: `python -m sans check path\\to\\script.sas --out out --tables in --strict`

Emits:
- `plan.ir.json`
- `report.json`

Refusal principle: unknown means no. Unsupported constructs refuse the entire block with a stable code and source loc.

Report contract: `docs/REPORT_CONTRACT.md`

### Minimal example
```
data out;
  set in;
  x = a + 1;
  if x > 3;
run;
```

Exit line:
```
ok: wrote plan.ir.json report.json
```

## Hello world (copy/paste)
```
python -m sans check hello.sas --out out --tables in
python -m sans run hello.sas --out out --tables in=in.csv
```
Outputs:
- `out/plan.ir.json`
- `out/report.json`
- `out/out.csv`
