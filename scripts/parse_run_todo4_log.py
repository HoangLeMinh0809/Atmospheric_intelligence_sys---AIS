#!/usr/bin/env python3
"""Parse run_todo4_stack raw logs into step and Spark job summaries."""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


STEP_RE = re.compile(r"^===\s+(?P<name>.+?)\s+===$")
STEP_OK_RE = re.compile(r"^\[OK\]\s+(?P<name>.+)$")
STEP_SKIP_RE = re.compile(r"^\[SKIP\]\s+(?P<message>.+)$")
SPARK_SUBMIT_RE = re.compile(r"submit_spark_k8s\.sh\s+(?P<job>[a-z0-9-]+)")
SPARK_FAILED_ONCE_RE = re.compile(r"^\[WARN\]\s+Spark K8s job failed once:\s+(?P<job>[a-z0-9-]+)")
SPARK_BEST_EFFORT_FAIL_RE = re.compile(r"^\[WARN\]\s+Best-effort job failed:\s+(?P<job>[a-z0-9-]+)\s+--\s+(?P<message>.+)$")
JOB_FAILED_RE = re.compile(r"^\[ERROR\]\s+K8s submit job failed:\s+(?P<job>[a-z0-9-]+)")
GENERIC_ERROR_RE = re.compile(r"(failed|error|exception|traceback)", re.IGNORECASE)
SPARK_STATUS_RE = re.compile(r"\bstatus=(?P<status>[a-zA-Z0-9_:-]+)")
TABLE_ROWS_RE = re.compile(r"\btable_rows=(?P<table_rows>\d+)")
COUNT_FIELD_RE = re.compile(r"\b(?P<key>[a-zA-Z_]+count|[a-zA-Z_]+_count)=(?P<value>\d+)")


@dataclass
class SparkJob:
    job: str
    step: str | None
    first_line: int
    last_line: int
    attempts: int = 1
    result: str = "submitted"
    status_tokens: list[str] = field(default_factory=list)
    metrics: dict[str, int] = field(default_factory=dict)
    messages: list[str] = field(default_factory=list)


@dataclass
class StepSummary:
    name: str
    start_line: int
    end_line: int | None = None
    result: str = "running"
    spark_jobs: list[str] = field(default_factory=list)
    warnings: int = 0
    errors: int = 0
    messages: list[str] = field(default_factory=list)


def update_job_from_line(job: SparkJob, line: str, line_no: int) -> None:
    job.last_line = line_no
    status = SPARK_STATUS_RE.search(line)
    if status:
        token = status.group("status")
        if token not in job.status_tokens:
            job.status_tokens.append(token)
        if token in {"written", "dry_run_success", "success", "completed", "ok"}:
            job.result = "success"
        elif "fail" in token or "error" in token:
            job.result = "failed"
    table_rows = TABLE_ROWS_RE.search(line)
    if table_rows:
        job.metrics["table_rows"] = int(table_rows.group("table_rows"))
    for match in COUNT_FIELD_RE.finditer(line):
        job.metrics[match.group("key")] = int(match.group("value"))


def parse_log(path: Path) -> dict[str, Any]:
    steps: list[StepSummary] = []
    jobs: list[SparkJob] = []
    current_step: StepSummary | None = None
    current_job: SparkJob | None = None
    raw_lines = path.read_text(encoding="utf-8", errors="replace").splitlines()

    for idx, line in enumerate(raw_lines, start=1):
        clean_line = line.strip().lstrip("\ufeff")
        step_match = STEP_RE.match(clean_line)
        if step_match:
            if current_step and current_step.end_line is None:
                current_step.end_line = idx - 1
                if current_step.result == "running":
                    current_step.result = "unknown"
            current_step = StepSummary(name=step_match.group("name"), start_line=idx)
            steps.append(current_step)
            current_job = None
            continue

        if current_step:
            if "[WARN]" in line:
                current_step.warnings += 1
            if "[ERROR]" in line or "Traceback" in line:
                current_step.errors += 1
            ok_match = STEP_OK_RE.match(clean_line)
            if ok_match and ok_match.group("name") == current_step.name:
                current_step.result = "success"
                current_step.end_line = idx
            skip_match = STEP_SKIP_RE.match(clean_line)
            if skip_match:
                current_step.messages.append(skip_match.group("message"))

        submit_match = SPARK_SUBMIT_RE.search(line)
        if submit_match:
            job_name = submit_match.group("job")
            current_job = SparkJob(job=job_name, step=current_step.name if current_step else None, first_line=idx, last_line=idx)
            jobs.append(current_job)
            if current_step and job_name not in current_step.spark_jobs:
                current_step.spark_jobs.append(job_name)

        failed_once = SPARK_FAILED_ONCE_RE.match(clean_line)
        if failed_once:
            job_name = failed_once.group("job")
            target = next((job for job in reversed(jobs) if job.job == job_name), None)
            if target:
                target.attempts += 1
                target.result = "retried"
                target.messages.append("failed once; repaired HDFS path and retried")

        best_effort_fail = SPARK_BEST_EFFORT_FAIL_RE.match(clean_line)
        if best_effort_fail:
            job_name = best_effort_fail.group("job")
            target = next((job for job in reversed(jobs) if job.job == job_name), None)
            if target:
                target.result = "best_effort_failed"
                target.messages.append(best_effort_fail.group("message")[:500])

        job_failed = JOB_FAILED_RE.match(clean_line)
        if job_failed:
            job_name = job_failed.group("job")
            target = next((job for job in reversed(jobs) if job.job == job_name), None)
            if target:
                target.result = "failed"

        if current_job:
            update_job_from_line(current_job, line, idx)
            if GENERIC_ERROR_RE.search(line) and len(current_job.messages) < 8:
                current_job.messages.append(line.strip()[:500])

    if current_step and current_step.end_line is None:
        current_step.end_line = len(raw_lines)
        if current_step.result == "running":
            current_step.result = "unknown"

    for job in jobs:
        if job.result in {"submitted", "retried"} and any(token in {"written", "success", "dry_run_success"} for token in job.status_tokens):
            job.result = "success"

    return {
        "log_path": str(path),
        "line_count": len(raw_lines),
        "steps": [asdict(step) for step in steps],
        "spark_jobs": [asdict(job) for job in jobs],
        "summary": {
            "step_count": len(steps),
            "spark_job_count": len(jobs),
            "failed_spark_jobs": [job.job for job in jobs if "failed" in job.result],
            "warning_steps": [step.name for step in steps if step.warnings],
            "error_steps": [step.name for step in steps if step.errors],
        },
    }


def print_markdown(parsed: dict[str, Any]) -> None:
    print(f"# TODO4 Log Summary\n")
    print(f"- Log: `{parsed['log_path']}`")
    print(f"- Lines: {parsed['line_count']}")
    print(f"- Steps: {parsed['summary']['step_count']}")
    print(f"- Spark jobs: {parsed['summary']['spark_job_count']}\n")
    print("## Steps")
    for step in parsed["steps"]:
        jobs = ", ".join(step["spark_jobs"]) if step["spark_jobs"] else "-"
        print(f"- `{step['result']}` lines {step['start_line']}-{step['end_line']}: {step['name']} | Spark: {jobs}")
    print("\n## Spark Jobs")
    for job in parsed["spark_jobs"]:
        metrics = ", ".join(f"{key}={value}" for key, value in sorted(job["metrics"].items())) or "-"
        print(f"- `{job['result']}` {job['job']} step=`{job['step']}` attempts={job['attempts']} metrics: {metrics}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse run_todo4_stack raw log into a step/Spark-job summary")
    parser.add_argument("log_path", nargs="?", default="logs/run_todo4_stack.raw.log")
    parser.add_argument("--format", choices=["json", "markdown"], default="json")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    parsed = parse_log(Path(args.log_path))
    if args.format == "markdown":
        rendered = None
        if args.output:
            from io import StringIO
            import contextlib

            buffer = StringIO()
            with contextlib.redirect_stdout(buffer):
                print_markdown(parsed)
            rendered = buffer.getvalue()
        else:
            print_markdown(parsed)
            return
    else:
        rendered = json.dumps(parsed, ensure_ascii=False, indent=2)

    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    else:
        print(rendered)


if __name__ == "__main__":
    main()
