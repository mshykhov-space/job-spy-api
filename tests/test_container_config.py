import unittest
from pathlib import Path


class ContainerConfigTest(unittest.TestCase):
    def test_uvicorn_access_log_is_disabled_to_protect_proxy_credentials(self) -> None:
        dockerfile = (Path(__file__).parents[1] / "Dockerfile").read_text()
        command = next(line for line in dockerfile.splitlines() if line.startswith("CMD "))

        self.assertIn("--no-access-log", command)


if __name__ == "__main__":
    unittest.main()
