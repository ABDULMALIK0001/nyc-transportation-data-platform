"""Build a Spark Docker command that works locally and from Airflow."""

import os
from pathlib import Path
import socket
import subprocess

from src.config import settings


SPARK_IMAGE = "spark:4.1.2-python3"


def project_mount_source() -> str:
    """Return the host project path understood by the Docker daemon."""
    override = os.getenv("DOCKER_PROJECT_PATH")
    if override:
        return override

    if settings.project_root.as_posix() == "/opt/project":
        result = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                '{{range .Mounts}}{{if eq .Destination "/opt/project"}}'
                "{{.Source}}{{end}}{{end}}",
                socket.gethostname(),
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()

    return str(settings.project_root)


def build_spark_submit_command(
    job_path: str,
    job_arguments: list[str],
    *,
    driver_memory: str = "4g",
    shuffle_partitions: int = 8,
) -> list[str]:
    """Build an isolated local Spark submission using the official image."""
    if not Path(settings.project_root / job_path).exists():
        raise ValueError(f"Spark job does not exist: {job_path}")
    return [
        "docker",
        "run",
        "--rm",
        "--user",
        "0:0",
        "--mount",
        f"type=bind,source={project_mount_source()},target=/opt/project",
        "--workdir",
        "/opt/project",
        "--env",
        "SPARK_LOCAL_IP=127.0.0.1",
        SPARK_IMAGE,
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[4]",
        "--driver-memory",
        driver_memory,
        "--conf",
        f"spark.sql.shuffle.partitions={shuffle_partitions}",
        f"/opt/project/{job_path}",
        *job_arguments,
    ]
