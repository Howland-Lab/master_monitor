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

When the current job is no longer safe to continue, the script prepares a restart and submits a child job so the simulation can continue automatically.

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

### Unified memory sampling and guard logic

The script samples Slurm memory statistics once per main monitor cycle via `sstat`. The same sample is used both to append a row to the per-job memory CSV and to drive the in-loop memory-guard projection logic.

### Restart lock protection

A lock file prevents duplicate restart actions from competing trigger paths such as timeout logic, memory-guard logic, and SIGTERM handling.

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
* path to the shared master monitor body,
* launcher function such as `mpirun`, `ibrun`, or `srun`.

This local script is the actual object submitted with `sbatch`, and it is also the script resubmitted for all child jobs.

---

## How It Works

### 1. Local job script defines the case

The local job script sets:

* Slurm directives,
* case-specific variables,
* the platform-specific `run_job()` function,
* paths to the input file, solver, environment setup file, and master monitor body.


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

The next job in the chain is submitted through Slurm.

### 8. The current simulation is terminated cleanly

After successful child submission, the current MPI launcher is terminated and the job exits.

---

## Memory Sampling and CSV Output

Memory is sampled once per monitor cycle via `sstat`.

Each successful sampling cycle appends rows to a per-job CSV file of the form:

```text
memdiag_job${SLURM_JOB_ID}.csv
```

The same `sstat` sample is used for two purposes:

1. appending memory diagnostics to the CSV file,
2. evaluating whether memory growth is projected to hit an unsafe threshold soon enough to justify a proactive restart.

This keeps the logging and the restart decision logic synchronized.

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
3. the Slurm output log is correctly identified,
4. elapsed-step timings are being parsed,
5. restart files are detected correctly,
6. input-file restart fields are edited as expected,
7. child jobs resubmit the same local case script,
8. the environment setup works in both parent and child jobs,
9. the memory CSV is being written as expected.

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