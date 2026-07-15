"""Unit tests for the Docker-based Spark submission command."""

from src.common.spark_runner import build_spark_submit_command


def test_builds_isolated_spark_command(monkeypatch) -> None:
    monkeypatch.setenv("DOCKER_PROJECT_PATH", "C:/example/project")

    command = build_spark_submit_command(
        "spark/jobs/silver_trips.py", ["--year", "2024", "--month", "8"]
    )

    assert command[:3] == ["docker", "run", "--rm"]
    assert "type=bind,source=C:/example/project,target=/opt/project" in command
    assert "spark:4.1.2-python3" in command
    assert command[-4:] == ["--year", "2024", "--month", "8"]
