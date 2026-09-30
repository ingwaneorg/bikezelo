"""
Best-effort pings to qapha, the course's run tracker, so the tutor can see which
VMs have run setup, started the app, and breached the SLA.

The VM is identified by its hostname. LOD VMs all share the same hostname, so
those get a random suffix, kept in data/ so it survives setup_db.py resets.

Only course VMs ping - this repo is public, so anyone running it elsewhere
(including the author's own machine) shouldn't send their details anywhere.
"""
import os
import re
import getpass
import secrets
import socket
import requests

QAPHA_URL = "https://qapha-249748487450.us-east1.run.app/"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
VM_ID_PATH = os.path.join(BASE_DIR, "data", "vm_id.txt")

SHARED_HOSTNAME = "MIS"  # every LOD VM reports this

# Learner VMs, the instructor VM, and LOD VMs
COURSE_HOSTNAME = re.compile(r"^(STUDENT\d+|INSTRUCTOR|MIS)$", re.IGNORECASE)


def vm_name():
    host = socket.gethostname()
    if host.upper() != SHARED_HOSTNAME:
        return host

    try:
        with open(VM_ID_PATH) as f:
            suffix = f.read().strip()
    except OSError:
        suffix = ""

    if not suffix:
        suffix = secrets.token_hex(2)
        try:
            os.makedirs(os.path.dirname(VM_ID_PATH), exist_ok=True)
            with open(VM_ID_PATH, "w") as f:
                f.write(suffix)
        except OSError:
            pass  # still usable for this run, just won't be the same next time

    return f"{host}-{suffix}"


def ping_qapha(source):
    # Best-effort only - a dead/unreachable endpoint must never break anything for a student.
    try:
        if not COURSE_HOSTNAME.match(socket.gethostname()):
            return
        ctx = {
            "currentNotebookName": "bikezelo",
            "currentWorkspaceName": vm_name(),
            "userName": getpass.getuser(),
            "source": source,
        }
        requests.post(QAPHA_URL, json=ctx, timeout=5)
    except Exception:
        pass
