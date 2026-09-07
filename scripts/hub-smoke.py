import http.client
import json
import os
import pathlib
import queue
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading


def run(binary):
    with tempfile.TemporaryDirectory(prefix="detent-hub-smoke-") as directory:
        root = pathlib.Path(directory)
        installed = root / "bin" / "detent"
        installed.parent.mkdir()
        shutil.copy2(binary, installed)
        token = secrets.token_hex(32)
        environment = {
            "PATH": os.environ.get("PATH", ""),
            "HOME": str(root),
            "TMPDIR": str(root),
            "DETENT_HUB_ADMIN_TOKEN": token,
        }
        database = root / "data" / "hub.db"
        snapshot = root / "backup.db"
        restored = root / "restored.db"

        def command(*args):
            result = subprocess.run(
                [str(installed), "hub", *args],
                env=environment,
                capture_output=True,
                timeout=30,
                check=True,
            )
            for credential in (token, environment["DETENT_HUB_ADMIN_TOKEN"]):
                assert credential.encode() not in result.stdout + result.stderr
            return result.stdout

        def request(port, credential):
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            try:
                connection.request("GET", "/health", headers={"Authorization": "Bearer " + credential})
                response = connection.getresponse()
                body = json.loads(response.read())
                return response.status, body
            finally:
                connection.close()

        def serve(path, rejected=None):
            addresses = queue.Queue()
            startup = []
            process = subprocess.Popen(
                [str(installed), "hub", "serve", "--database", str(path),
                 "--listen", "127.0.0.1:0", "--github-disabled"],
                env=environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )

            def read_address():
                for line in process.stdout:
                    startup.append(line)
                    match = re.search(r"127\.0\.0\.1:(\d+)", line)
                    if match:
                        addresses.put(int(match.group(1)))

            reader = threading.Thread(target=read_address, daemon=True)
            reader.start()
            try:
                try:
                    port = addresses.get(timeout=30)
                except queue.Empty:
                    raise RuntimeError("Hub did not report its listener: " + "".join(startup)) from None
                status, body = request(port, environment["DETENT_HUB_ADMIN_TOKEN"])
                assert status == 200 and body["status"] == "ok", (status, body)
                assert body["schema_version"] > 0
                if rejected:
                    assert request(port, rejected)[0] == 401
            finally:
                process.terminate()
                process.wait(timeout=15)
                reader.join(timeout=5)
                process.stdout.close()
            assert process.returncode == 0, process.returncode

        serve(database)
        serve(database)
        command("backup", "--database", str(database), "--output", str(snapshot))
        verified = json.loads(command("verify", "--database", str(snapshot)))
        assert verified["schema_version"] > 0
        environment["DETENT_HUB_ADMIN_TOKEN"] = secrets.token_hex(32)
        result = json.loads(command("restore", "--database", str(snapshot), "--output", str(restored)))
        assert result["administrator_id"].startswith("restore-admin-")
        serve(restored, rejected=token)
        command("verify", "--database", str(restored))
        print("Hub isolated installation, restart, backup, restore and credential fencing passed")


if __name__ == "__main__":
    run(pathlib.Path(sys.argv[1]).resolve())
