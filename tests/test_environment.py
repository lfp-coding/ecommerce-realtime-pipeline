import subprocess
from pathlib import Path


import docker
import pytest


class TestDevelopmentEnvironment:
    """
    Tests for validating the development environment setup.
    """

    def test_docker_daemon_accessible(self):
        """Docker daemon should be accessible."""
        try:
            client = docker.from_env()
            assert client.ping() is True, "Docker daemon not accessible"
        except Exception as e:
            pytest.fail(f"Docker daemon not accessible: {e}")

    def test_docker_cli_available(self):
        """Docker CLI should be available and running in Linux mode."""
        result = subprocess.run(["docker", "version"], capture_output=True, text=True)
        assert result.returncode == 0, "Docker CLI not available"
        assert "linux" in result.stdout.lower(), (
            "Docker should be running in Linux mode"
        )

    def test_project_structure(self):
        """Project should have a modern structure with required directories."""
        required_dirs = [
            "src/config",
            "src/consumer",
            "src/data_generator",
            "src/monitoring",
            "src/dashboard",
            "tests/unit",
            "tests/integration",
            "docs",
            "sql",
            "data/sample",
            "scripts",
        ]
        for dir_path in required_dirs:
            assert Path(dir_path).is_dir(), f"Missing directory: {dir_path}"

    def test_python_dependencies_importable(self):
        """All required Python packages should be importable."""
        packages = ["confluent_kafka", "psycopg2", "pandas", "streamlit", "pytest"]
        for pkg in packages:
            try:
                __import__(pkg)
            except ImportError:
                pytest.fail(f"Missing Python package: {pkg}")
