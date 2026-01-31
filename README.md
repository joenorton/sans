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

Supported subset (current):
- data steps: `set`/`merge` with assignments, `if`, `keep/drop/rename`, BY-group flags, retain, and dataset options (`keep/drop/rename/where`, `in=` on merge)
- `proc sort` (including `nodupkey`, last wins)
- `proc transpose` (by/id/var)
- `proc sql` subset (create table as select with inner/left joins, where, group by, aggregates)
- `proc format` (VALUE mappings + `put()` for lookups)
- `proc summary` (NWAY class means with `output out=... mean= / autoname`)
- `validate --profile sdtm` (DM/AE/LB rulepack)

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

## Validate (SDTM)
```
python -m sans validate --profile sdtm --out out --tables dm=dm.csv,ae=ae.csv,lb=lb.csv
```
Outputs:
- `out/validation.report.json`

## Testing
Pytest cache is disabled by default on Windows to avoid filesystem permission warnings.
