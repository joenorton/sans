you are codex. objective: implement a sas-lite execution subset in this repo so that a new “hello_galaxy” integration test passes end-to-end on windows, without requiring SAS.

constraints:
- keep scope strictly to what is needed for hello_galaxy.sas (below). do not implement full SAS or macros.
- determinism > permissiveness. no silent coercions. fail fast with actionable errors (include file+line if available).
- do not add network calls to tests; fixtures are local temp files.
- keep code style consistent with repo. update docs only if necessary.

definition of done (binary):
1) `python -m sans check hello_galaxy.sas --out out --tables dm=dm.csv ex=ex.csv lb=lb.csv` succeeds
2) `python -m sans run hello_galaxy.sas --out out --tables dm=dm.csv ex=ex.csv lb=lb.csv` succeeds
3) output table `lb_final` is produced and contains exactly 3 rows, matching expected values (with float tolerance for pchg)
4) new pytest integration test `test_hello_galaxy.py` passes, plus entire test suite remains green.

hello_galaxy.sas (must be runnable):
---
proc sort data=dm out=dm_s;
  by subjid;
run;

proc sort data=ex out=ex_s;
  by subjid exstdtc;
run;

proc sort data=lb out=lb_s;
  by subjid lbdtc;
run;

data ex_first;
  set ex_s;
  by subjid;
  if first.subjid then output;
  keep subjid exstdtc;
run;

data subj;
  merge dm_s(in=indm) ex_first(in=inex);
  by subjid;
  if indm and inex;
  keep subjid siteid sex race exstdtc;
run;

data lb_pre;
  merge lb_s(in=inlb) subj(in=insubj);
  by subjid;
  if inlb and insubj;
  if lbdtc <= exstdtc;
run;

proc sort data=lb_pre out=lb_pre_s;
  by subjid lbtestcd lbdtc;
run;

data base;
  set lb_pre_s;
  by subjid lbtestcd;
  retain baseval;
  if first.lbtestcd then baseval = .;
  baseval = lbstresn;
  if last.lbtestcd then output;
  keep subjid lbtestcd baseval;
run;

data lb_final;
  merge lb_s(in=inlb) subj(in=insubj) base(in=inbase);
  by subjid;
  if inlb and insubj and inbase;
  if lbdtc > exstdtc;

  chg = lbstresn - baseval;
  if baseval ne 0 then pchg = (chg / baseval) * 100;
  else pchg = .;

  keep subjid siteid sex race exstdtc lbdtc lbtestcd lbstresn baseval chg pchg;
run;
---

fixtures (csv content; use in test via temp files; no need to commit these as standalone files):
dm.csv:
subjid,siteid,sex,race
101,001,M,WHITE
102,001,F,BLACK OR AFRICAN AMERICAN
103,002,M,ASIAN

ex.csv:
subjid,exstdtc
101,2023-01-11
102,2023-01-12

lb.csv:
subjid,lbdtc,lbtestcd,lbstresn
101,2023-01-10,GLUC,95
101,2023-01-11,GLUC,96
101,2023-01-13,GLUC,110
101,2023-01-20,GLUC,100
102,2023-01-10,GLUC,88
102,2023-01-12,GLUC,90
102,2023-01-15,GLUC,91
103,2023-01-10,GLUC,77

expected semantics and output for lb_final:
- subject 103 excluded entirely (no ex record)
- for subjid 101: exstdtc=2023-01-11, baseline=96 (last pre-dose value at lbdtc<=exstdtc), post-dose rows lbdtc>exstdtc are:
  - 2023-01-13 lbstresn=110 => chg=14, pchg=(14/96)*100=14.5833333333
  - 2023-01-20 lbstresn=100 => chg=4,  pchg=(4/96)*100=4.1666666667
- for subjid 102: exstdtc=2023-01-12, baseline=90, post-dose row:
  - 2023-01-15 lbstresn=91 => chg=1, pchg=(1/90)*100=1.1111111111
- lb_final should have 3 rows total; column set (order doesn’t matter, but keep should preserve order if easy):
  subjid,siteid,sex,race,exstdtc,lbdtc,lbtestcd,lbstresn,baseval,chg,pchg

required language/runtime features to implement (only these):
A) proc sort:
- parse `proc sort data=<in> out=<out>; by <vars...>; run;`
- runtime: stable sort by keys, output new table
B) data step:
- parse `data <out>; ... run;`
- support:
  - `set <table>;`
  - `merge <t1>(in=<flag>) <t2>(in=<flag>) ...; by <keys...>;`
  - `by <keys...>;` inside data step (requires input sorted by those keys; error otherwise)
  - computed vars: assignments `x = expr;`
  - comparisons and boolean ops: `> < <= >= =`, `and/or`, parentheses
  - if filter: `if expr;` (drop row if false/missing)
  - if-then-else: `if expr then <stmt>; else <stmt>;` where stmt can be assignment or `output`
  - `output;` to emit current row (otherwise default: emit all rows unless filtered; model SAS data step default)
  - `retain <var>;` with SAS-like behavior (value persists across rows; initialize missing `.`)
  - special missing value token `.` (numeric missing)
  - `keep <vars...>;` applies at end of step to projected columns
  - automatic variables `first.<key>` and `last.<key>` when `by` is active (true at group boundaries)
C) merge semantics:
- implement a simple merge-by-keys:
  - assume each input is sorted by the BY keys; error if not sorted
  - handle 1:1 and 1:many by emitting multiple rows appropriately OR, if too complex, explicitly forbid duplicates by key for this sprint and error with clear message. (choose simplest path that still lets hello_galaxy pass; current fixtures have unique keys for dm/ex_first/subj/base, lb has many per subjid but in merges by subjid should work as 1:many. so you must support at least one-many where the “driving” table has multiple rows and the others provide last-seen match for the key.)
  - set in= flags per row indicating whether that input contributed a row for the current emitted row
  - for fields from non-contributing tables, set missing
  - implement merge iteration in a deterministic way; document in code comments
D) types:
- parse numbers as float
- treat ISO8601 dates as strings; comparisons like `lbdtc <= exstdtc` work lexicographically
- boolean missing: treat missing as false for filters? better: SAS treats missing numeric as false in comparisons; implement: if any operand missing => comparison is false, so filter drops row.

implementation guidance:
- first inspect current parser/ir/runtime modules. extend IR with SortOp, DataStepOp, MergeByOp, KeepOp, RetainOp or similar.
- do not intertwine parsing with execution; keep a compile -> plan -> execute pipeline consistent with repo.
- add precise errors for unsupported statements.
- ensure `sans check` validates constructs and reports unsupported features before runtime.

tests:
- add `tests/test_hello_galaxy.py`:
  - write hello_galaxy.sas + the three csvs into tmp_path
  - run `python -m sans check ...` and `python -m sans run ...` via subprocess
  - locate output file for lb_final (use whatever output convention repo uses; inspect existing runtime tests)
  - load output CSV and assert:
    - row count == 3
    - per subjid/lbdtc expected baseval/chg/pchg within tolerance (1e-6)
    - subject 103 absent
    - required columns exist
- also add a few focused unit tests if necessary for merge/by-group first/last.

deliver:
- implement features
- update report schema/contract only if required
- keep changes minimal but robust
- provide a short final summary: files changed + how to run the test.

begin now.
