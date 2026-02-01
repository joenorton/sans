# sans

Small, deterministic compiler **and executor** for a strict SAS‑like batch subset. It emits a plan and report, executes the supported subset, and refuses anything outside scope with stable error codes + file/line.

Current stance on execution:
- **Strict by default**: unknown means no; unsupported constructs refuse the entire block.
- **Deterministic**: stable row/column order and reproducible artifacts.
- **Bounded**: no macro engine, no networking, no dynamic codegen.

## Quickstart (prioritized)
1) Install
```
python -m pip install -e .
```

2) Compile + validate (check only)
```
python -m sans check path\to\script.sas --out out --tables in --strict
```
Emits:
- `out/plan.ir.json`
- `out/report.json`
- `out/preprocessed.sas` (macro‑lite expansion)

3) Compile the native `.sans` script
```
cd demo
python -m sans check sans_script/demo.sans --out out_sans --tables in
```
Emits:
- `demo/out_sans/plan.ir.json`
- `demo/out_sans/report.json`

4) Execute a richer demo
```
cd demo
./demo.sh        # or powershell ./demo.ps1
```
Emits (inside `demo/out`):
- `plan.ir.json`
- `report.json`
- `final.csv`
```

5) Optional: execute to XPT
```
cd demo
python -m sans run hello.sas --out out --tables in=in.csv --format xpt
```
Emits:
- `demo/out/out.xpt`

6) Validate (SDTM)
```
python -m sans validate --profile sdtm --out out --tables dm=dm.csv,ae=ae.csv,lb=lb.csv
```
Emits:
- `out/validation.report.json`

7) Verify reproducibility
```
python -m sans verify out
```

## Native `.sans` DSL

- First write a `.sans` script with `format`, `data`, `sort`, `summary`, and `select` statements, then run `python -m sans check` to produce the same `plan.ir.json` + `report.json` bundle as SAS.
- Each statement carries a deterministic `step_id` and reuses the same expression AST/validation logic as the SAS compiler, which means the DSL can be used interchangeably in downstream tooling.
- Try it out with `demo/sans_script/demo.sans` (compiled as part of the quickstart above) to see the native DSL plan, or hook it into your own pipeline to emit canonical plans without touching SAS.
- The DSL uses `from ... do ... end` blocks for input modifiers, `filter(...)` for row filtering, `==` for equality, `->` for mappings, and `=` for assignments. Overwrite requires `derive!`.
- Grammar reference: `sans/sans/sans_script/docs/grammar.md`

## XPT parity
- `sans run --format xpt` now uses deterministic padding/length inference (cap 200), trims trailing spaces on read and pads on write, emits `ok_warnings` when ignoring labels/formats, and hashes bytes via `sans verify`. See `docs/SUBSET_SPEC.md` for canonicalization rules.

## Supported subset (current)
- DATA step: `set`/`merge` with assignments, `if`, `keep/drop/rename`, BY‑group flags, retain, dataset options (`keep/drop/rename/where`, `in=` on merge)
- Macro‑lite: `%let`, `%include`, `&VAR`, single‑line `%if/%then/%else` (no `%do/%end`, no `%macro`)
- `proc sort` (including `nodupkey`, last wins)
- `proc transpose` (by/id/var)
- `proc sql` subset (create table as select with inner/left joins, where, group by, aggregates)
- `proc format` (VALUE mappings + `put()` for lookups)
- `proc summary` (NWAY class means with `output out=... mean= / autoname`)
- `validate --profile sdtm` (DM/AE/LB rulepack)
- `.sans` DSL: format/data/sort/summary/select statements that compile into the deterministic `plan.ir.json` plan.

## Minimal example
```
data out;
  set in;
  x = a + 1;
  if x > 3;
run;
```

## Notes
- `%include` is restricted to the script directory + `--include-root` entries. Absolute paths require `--allow-absolute-include`.

## Known limits
- Macro control flow is single‑line only (`%if/%then/%else`); `%do/%end` and `%macro` are unsupported.
- No PROC SQL beyond the documented subset (no subqueries, unions, or window functions).
- No networking, no external systems, no dynamic code generation.
- XPT support is minimal but deterministic; not a full SAS transport implementation.

## Why strict?
- Prevents semantic creep and “mostly works” behavior.
- Keeps outputs deterministic and reviewable.
- Makes refusals explicit so you can decide how to extend the subset safely.

## Deep references
- Subset spec: `docs/SUBSET_SPEC.md`
- Report schema: `docs/REPORT_CONTRACT.md`
- Error codes: `docs/ERROR_CODES.md`
- Architecture: `docs/ARCHITECTURE.md`
- Roadmap / sprints: `docs/sprints/README.md`
- Vision / pathway: `docs/BIG_PIC.md`, `docs/PATHWAY.md`
