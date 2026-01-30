## mission brief — project sans

### mission

build **sans**, a small, fast, standalone interpreter for legacy-style batch data scripts that reliably executes the *useful core* of common data workflows while failing loudly and honestly on everything else. sans exists to deliver deterministic, inspectable results without dragging in the weight, opacity, or operational overhead of full statistical platforms.

### problem

most legacy data scripts are boring in the best sense: load tabular data, compute columns, filter rows, sort, summarize, compare outputs. yet the ecosystems that run them are sprawling, fragile, and hostile to portability. teams pay for complexity they don’t use, accept silent semantic drift, and debug outputs they can’t explain. replacing those platforms wholesale is unrealistic; running them forever is worse.

### thesis

80% of real-world batch data work can be covered by a sharply bounded execution model with explicit semantics and zero magic. correctness beats completeness. unsupported features are not bugs if they fail clearly and early.

### scope

sans intentionally supports only:

* file-based batch execution (no interactivity)
* tabular data operations: load, keep/drop, compute, filter, sort
* essential procedures: summary statistics, frequency counts, dataset comparison
* deterministic output with stable ordering and explicit numeric tolerance

sans explicitly does **not** support:

* databases, connectors, or network I/O
* user-defined macros, functions, or control-flow abstractions
* plotting, visualization, or exploratory analysis
* full language compatibility with any existing platform

### architectural stance

sans is a **single executable** with a **strict internal boundary**:

1. preprocess (simple variable substitution, comment stripping)
2. parse into a flat intermediate representation (IR)
3. validate the IR for semantic correctness
4. execute against a vectorized runtime
5. emit human-readable logs and machine-usable outputs

parsing and execution are never interleaved. nothing touches data until validation passes.

### correctness doctrine

* every operation declares its soundness: sound, approximate, or unsupported
* approximate behavior is always warned and marked
* unsupported behavior halts unless explicitly forced
* no silent fallbacks that can change results

if sans produces output, it must be explainable, repeatable, and defensible.

### success criteria

* a new user can run a simple script to completion in under five minutes
* scripts either succeed correctly or fail clearly—never “mostly work”
* startup is fast, memory usage is bounded, behavior is deterministic
* the codebase remains small enough to be understood end-to-end by one competent engineer

### non-goals

sans is not a drop-in replacement for enterprise analytics stacks. it is not a teaching tool, a REPL, or a research platform. it will never chase feature parity.

### north star

make boring data work boring again—predictable, portable, and trustworthy—by refusing to pretend that complexity is value.

---
