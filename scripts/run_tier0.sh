#!/usr/bin/env bash
set -euo pipefail

python -m pytest \
  sans/tests/test_hello_galaxy.py \
  sans/tests/test_hello_universe.py \
  sans/tests/test_hello_cosmos.py \
  sans/tests/test_hello_multiverse.py \
  sans/tests/test_hello_verify.py \
  sans/tests/test_hello_xpt.py \
  sans/tests/test_hello_macro_m0.py \
  sans/tests/test_data_step_compiler.py \
  sans/tests/test_runtime.py \
  sans/tests/test_sql.py \
  sans/tests/test_proc_format_summary.py \
  sans/tests/test_validate_sdtm.py
