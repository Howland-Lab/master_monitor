# Slurm Job Monitor and Auto-Resubmission

A Bash-based monitoring framework for long-running Slurm jobs that need to survive wall-time limits, stalled execution, and unsafe memory growth by restarting from the most recent valid restart files and automatically submitting the next job in the chain.

---

## Overview

Long-running simulations on HPC systems often cannot finish within a single wall-time allocation. For restartable workflows, this usually means users must either:

* monitor jobs manually and resubmit them before timeout,
* risk losing progress when a job reaches wall-time,
* or build fragile custom chaining logic for each case.

This repository provides a reusable Bash monitoring layer for Slurm-based simulations that solves that problem. It is designed for workflows that:

* write restart files periodically,
* use one or more coupled simulations that must restart from a common valid time index,
* run on systems where wall-time limits require job chaining,
* benefit from early detection of frozen execution or unsafe memory growth.

The script is intended to be sourced by a case-specific Slurm job script. The local job script defines the Slurm directives, case-specific paths, launcher, and monitoring settings. The shared master monitor body then handles execution, monitoring, restart preparation, and child-job submission.

## Scope and Origin

This monitoring framework was originally developed for restartable [PadeOps](https://github.com/Howland-Lab/PadeOps) workflows in the Howland Lab. Its current structure reflects that use case, including assumptions about solver launch, progress logging, restart-file availability, and input-file updates.

That said, the overall design is not specific to PadeOps alone. With modest changes to the launcher, log parsing, restart-file detection, and restart-parameter editing logic, the same framework can be adapted to other restartable simulation codes running on Slurm systems.

---

## Main Features

### Automatic chained resubmission

When the current job is no longer safe to continue, the script prepares a restart and submits a dependent child job so the simulation can continue automatically.

### Restart-aware continuation

For coupled runs, the script searches for the latest **common restart time index** across the required restart streams before updating input files.

### Wall-time safety monitoring

The script estimates whether enough time remains to reach the next restart dump. If not, it triggers a controlled restart before the job is terminated by Slurm.

### Log-based timestep timing

The runtime estimate is taken preferentially from solver-reported elapsed-step timings found in the output log, using a rolling average over the most recent samples.

### Fallback wall-clock estimation

If elapsed-step timings are not available in the log, the script falls back to a coarser estimate based on wall-clock time and observed step progression.

### Frozen-job detection

If the output log stops updating or no progress is detected for too long, the script assumes the job is stalled and triggers a restart.

### Memory growth monitoring

The script monitors live Slurm memory statistics and can trigger an early restart if node-level memory usage is projected to hit an unsafe threshold before the next monitoring interval.

### Restart lock protection

A lock file prevents duplicate restart actions from competing trigger paths such as timeout logic, memory guard logic, and SIGTERM handling.

### Backup and restore of input files

Before modifying restart-related input settings, the script makes backups so that failed update attempts can be reverted safely.

---

## Intended Use Case

This repository is aimed at restartable HPC simulations on Slurm systems, especially workflows with:

* long runtimes,
* periodic restart dumps,
* coupled primary/precursor or multi-stream execution,
* machine-specific launcher differences,
* a need for robust unattended job chaining.

It is not limited to one solver, but it assumes your simulation has the following characteristics:

1. it can restart from files,
2. it writes progress to a log file,
3. it has identifiable restart dump cadence,
4. restart metadata can be updated through editable input files.

---

## Repository Structure

A typical setup looks like this:

```text
repo/
├── master_monitor.sh
├── README.md
└── examples/
    └── local_case_job.sh
```

### `master_monitor.sh`

The shared monitoring and auto-resubmission logic.

This script is **not** submitted directly with `sbatch`. It is meant to be sourced by a case-specific Slurm job script.

### `local_case_job.sh`

A case-specific batch script that contains:

* `#SBATCH` directives,
* solver path,
* input-file path,
* environment setup file,
* monitoring parameters,
* launcher function such as `mpirun` or `ibrun`.

This local script is the actual object submitted with `sbatch`, and it is also the script resubmitted for all child jobs.

---

## How It Works

### 1. Local job script defines the case

The local job script sets:

* Slurm directives,
* case-specific variables,
* the platform-specific `run_job()` function,
* paths to the input file, solver, and environment setup.

At the end of the local script, it resolves its own path and sources the master monitor body.

### 2. Master body starts the simulation

The master script:

* sources the environment setup file,
* exports the solver and main input file,
* parses the local script for Slurm metadata such as wall time and output log naming,
* launches the simulation via `run_job()`.

### 3. Monitor loop watches the run

The monitor loop periodically checks:

* whether the output log exists and is updating,
* the latest reported simulation step index,
* recent elapsed-step timings from the log,
* remaining wall time,
* current and projected memory usage.

### 4. Restart trigger conditions are evaluated

A restart can be triggered by:

* hard wall-time cutoff,
* persistent estimate that the job cannot safely reach the next restart dump,
* frozen execution,
* unsafe projected memory growth,
* emergency signal trapping.

### 5. Restart files are located

When a restart is needed, the script finds the latest common valid restart time index across the required simulation streams.

### 6. Input files are updated

The script edits the input files so the next job starts from that restart point.

### 7. A child job is submitted

The next job in the chain is submitted through Slurm, optionally with dependency on the current job.

### 8. The current simulation is terminated cleanly

After successful child submission, the current MPI launcher is terminated and the job exits.

---

## Typical Local Job Script

A case-specific Slurm script may look like this:

```bash
#!/bin/bash
#SBATCH --nodes=56
#SBATCH --partition=wide
#SBATCH --ntasks-per-node=64
#SBATCH --cpus-per-task=1
#SBATCH --exclusive
#SBATCH --time=12:00:00
#SBATCH --job-name=spinfarm250
#SBATCH --output=output_.o%j
#SBATCH --error=error_.e%j

MAIN_INPUTFILE="/path/to/input_main.dat"
SOLVER="/path/to/solver_binary"
SOURCEDFILE="/path/to/setup_environment.sh"
MASTER_MONITOR_SCRIPT="/path/to/master_monitor.sh"

SAFETY_FACTOR=1.5
MONITOR_INTERVAL=60
MEMWATCH_INTERVAL=300
HARD_CUTOFF_SECONDS=600
FROZEN_TIMEOUT_SECONDS=7200
ESTIMATE_PERSISTENCE_SAMPLES=10
MEMORY_GUARD_ENABLED=1
MEMORY_GUARD_LOOKAHEAD_INTERVALS=2
MEMORY_GUARD_UTILIZATION=0.9
MEMORY_GUARD_NODE_LIMIT_GB=256
MEMORY_GUARD_PERSISTENCE_SAMPLES=2
MEMORY_RATE_WINDOW=4
ELAPSED_STEP_WINDOW=5

run_job() {
    mpirun -np "${SLURM_NTASKS}" "${solver}" "${inputFile}" &
    MPI_PID=$!
}

JOB_CONFIG_ABS="$(realpath "${BASH_SOURCE[0]}")"
JOB_CONFIG_DIR="$(dirname "${JOB_CONFIG_ABS}")"
JOB_CONFIG_BASE="$(basename "${JOB_CONFIG_ABS}")"

source "${MASTER_MONITOR_SCRIPT}"
```

Submit it with:

```bash
sbatch local_case_job.sh
```

---

## Key Configuration Variables

### Core paths

* `MAIN_INPUTFILE`
  Path to the main simulation input file.

* `SOLVER`
  Path to the solver executable.

* `SOURCEDFILE`
  Environment setup script for modules, paths, compilers, MPI, and dependencies.

* `MASTER_MONITOR_SCRIPT`
  Path to the shared monitoring script body.

### Runtime estimation

* `SAFETY_FACTOR`
  Multiplies the projected time needed to reach the next restart dump.

* `MONITOR_INTERVAL`
  How often the monitor loop wakes up.

* `ELAPSED_STEP_WINDOW`
  Number of recent elapsed-step timings to average from the output log.

* `ESTIMATE_PERSISTENCE_SAMPLES`
  Number of consecutive unsafe estimates required before a restart is triggered.

### Frozen-run protection

* `FROZEN_TIMEOUT_SECONDS`
  Maximum allowed time without meaningful progress before the run is treated as stalled.

### Hard wall-time protection

* `HARD_CUTOFF_SECONDS`
  Last-resort threshold below which restart is forced regardless of time-per-step estimate.

### Memory guard

* `MEMORY_GUARD_ENABLED`
  Enable or disable memory monitoring.

* `MEMORY_GUARD_LOOKAHEAD_INTERVALS`
  Number of monitor intervals to look ahead when projecting memory growth.

* `MEMORY_GUARD_UTILIZATION`
  Fraction of the inferred memory limit at which the run is considered unsafe.

* `MEMORY_GUARD_NODE_LIMIT_GB`
  Optional explicit node memory limit override.

* `MEMORY_GUARD_PERSISTENCE_SAMPLES`
  Number of consecutive unsafe memory projections required before restart.

* `MEMORY_RATE_WINDOW`
  Number of recent memory samples used to estimate growth rate.

---

## Assumptions

This script assumes:

* the simulation writes restart files periodically,
* restart files can be identified by predictable filenames,
* the input files contain editable restart-related parameters,
* the output log contains step progression and, ideally, elapsed-step timing,
* the Slurm environment provides job metadata through commands such as `scontrol`, `sstat`, and `sacct`.

---

## Log-Based Time-Per-Step Estimation

The preferred runtime estimate comes from lines in the solver output log of the form:

```text
Elapsed time is    47.3021759986877       seconds
```

The script scans a recent section of the log, extracts the most recent elapsed-step samples, and averages the last `ELAPSED_STEP_WINDOW` values. This produces a smoother and more realistic estimate than simply dividing monitor-loop wall time by observed step increments.

If these log lines are not available, the script falls back to a coarser estimate based on:

* current wall-clock time,
* previous wall-clock time,
* current step index,
* previous step index.

---

## Restart Logic

For coupled workflows, the script searches for the latest restart index that exists in both required restart streams. This prevents the next job from restarting one simulation from a point that is not available in the other.

Once a valid common restart point is found, the script updates the relevant input-file variables, typically including:

* whether restart mode is enabled,
* restart time index,
* restart run ID.

---

## Why Use This Instead of Native Slurm Chaining Alone

Simple Slurm dependency chaining is often not enough for restartable scientific workflows because it does not answer questions like:

* Is there actually enough time left to reach the next restart dump?
* Did the simulation freeze without exiting?
* Is memory trending toward failure before the next check?
* Are the required restart files available and synchronized across simulation streams?

This script adds that missing logic.

---

## Portability

The monitor logic is portable across Slurm systems, but the local launcher function may differ by machine.

Examples:

* Anvil:

  ```bash
  run_job() {
      mpirun -np "${SLURM_NTASKS}" "${solver}" "${inputFile}" &
      MPI_PID=$!
  }
  ```

* Stampede3:

  ```bash
  run_job() {
      ibrun --distribution=block:block "${solver}" "${inputFile}" &
      MPI_PID=$!
  }
  ```

The rest of the monitoring body can remain the same.

---

## Failure Modes and Safeguards

The script includes several defensive mechanisms:

* **restart lock file** to avoid duplicate resubmission,
* **backup copies** of edited input files,
* **retry logic** for `sbatch` submission,
* **fallback runtime estimate** if log timing is unavailable,
* **persistent-sample checks** to suppress reactions to noisy one-off events.

Still, users should validate the setup for their own workflow before relying on it for production chains.

---

## Recommended Validation Before Production Use

Before large runs, test the workflow on a short case and verify:

1. the local script submits successfully with `sbatch`,
2. the solver launches correctly,
3. the monitor log is written,
4. the output log is correctly identified,
5. elapsed-step timings are being parsed,
6. restart files are detected correctly,
7. input-file restart fields are edited as expected,
8. child jobs resubmit the same local case script,
9. the environment setup works in both parent and child jobs.

---

## Limitations

This repository is designed for restartable simulations and is not a general scheduler framework. In particular:

* it assumes a restart-file workflow,
* restart-field editing is solver-specific,
* restart-file naming conventions are currently workflow-specific,
* memory monitoring depends on the behavior of Slurm accounting tools,
* log parsing assumes recognizable progress and timing output.

Users working with different restart conventions may need to adapt helper functions accordingly.

---

## Contact

For questions, issues, or maintenance inquiries:

**[karimali@mit.edu](mailto:karimali@mit.edu)**

---

## Date

**April 8, 2026**
