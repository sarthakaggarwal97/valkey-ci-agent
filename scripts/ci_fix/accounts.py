"""Own and clean up the disposable operating-system accounts used by CI-fix."""

from __future__ import annotations

import argparse
import grp
import os
import pwd
import shutil
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from scripts.common.isolation import _CLEAN_SYSTEM_ENV, _root, quiesce_account

_HEADER = "ci-fix-accounts-v1"
_USERS = frozenset({"ci-fix-setup", "ci-fix-command", "ci-fix-agent"})


@dataclass(frozen=True)
class AccountRecord:
    """One fixed account name reserved by this workflow run."""

    name: str
    uid: int | None = None
    gid: int | None = None

    @property
    def created(self) -> bool:
        return self.uid is not None and self.gid is not None


def create_marked(state_file: Path, user: str) -> None:
    """Create one account, with ownership recorded around the OS mutation."""
    mark_pending(state_file, user)
    if sys.platform == "darwin":
        identity = _next_darwin_identity()
        for argv in (
            ("/usr/bin/dscl", ".", "-create", f"/Groups/{user}", "PrimaryGroupID", str(identity)),
            ("/usr/bin/dscl", ".", "-create", f"/Users/{user}"),
            ("/usr/bin/dscl", ".", "-create", f"/Users/{user}", "UniqueID", str(identity)),
            ("/usr/bin/dscl", ".", "-create", f"/Users/{user}", "PrimaryGroupID", str(identity)),
            ("/usr/bin/dscl", ".", "-create", f"/Users/{user}", "UserShell", "/bin/bash"),
            (
                "/usr/bin/dscl",
                ".",
                "-create",
                f"/Users/{user}",
                "NFSHomeDirectory",
                f"/Users/{user}",
            ),
        ):
            _run_root(*argv)
    else:
        _run_root(
            _program("useradd"),
            "--system",
            "--user-group",
            "--no-create-home",
            "--home-dir",
            "/nonexistent",
            "--shell",
            "/bin/bash",
            user,
        )
        if _has_subordinate_ids(user):
            raise RuntimeError("the disposable CI-fix account received subordinate IDs")
    mark_created(state_file, user)


def mark_pending(state_file: Path, user: str) -> None:
    """Reserve ``user`` only after proving its user and group names are free."""
    _validate_user(user)
    records = _load(state_file, missing_ok=True)
    if user in records:
        raise ValueError(f"{user} is already recorded for this CI-fix run")
    if _user(user) is not None or _group(user) is not None:
        raise RuntimeError(f"the disposable account name {user} is already in use")
    records[user] = AccountRecord(user)
    _store(state_file, records)


def mark_created(state_file: Path, user: str) -> None:
    """Atomically bind a pending record to its completed numeric identity."""
    records = _load(state_file)
    record = records.get(user)
    if record is None or record.created:
        raise ValueError(f"{user} is not a single pending CI-fix account")
    account = _user(user)
    group = _group(user)
    if (
        account is None
        or group is None
        or account.pw_uid <= 0
        or group.gr_gid <= 0
        or account.pw_gid != group.gr_gid
    ):
        raise RuntimeError(f"the disposable account {user} was not created consistently")
    records[user] = AccountRecord(user, account.pw_uid, group.gr_gid)
    _store(state_file, records)


def quiesce_marked(state_file: Path, user: str | None = None) -> None:
    """Stop only numeric identities proven to have been created by this run."""
    records = _load(state_file)
    selected = records.values() if user is None else (_record(records, user),)
    for record in selected:
        _validate_live_identity(record)
        if not record.created:
            raise RuntimeError(f"the disposable account {record.name} was not fully created")
    for record in selected:
        assert record.uid is not None
        quiesce_account(record.name, expected_uid=record.uid)


def cleanup_marked(state_file: Path) -> None:
    """Quiesce and delete only exact identities recorded by this workflow run.

    A pending record never authorizes deletion. If cancellation interrupted
    account creation, cleanup fails closed and leaves the ephemeral runner to
    discard that partial identity.
    """
    records = _load(state_file)
    for record in records.values():
        _validate_live_identity(record)

    for record in records.values():
        if record.created:
            assert record.uid is not None
            quiesce_account(record.name, expected_uid=record.uid)

    for record in records.values():
        if not record.created:
            continue
        _validate_live_identity(record)
        if _user(record.name) is not None:
            _delete_user(record.name)
        # Linux userdel commonly removes the matching private group.
        if _group(record.name) is not None:
            _delete_group(record.name)
        if _user(record.name) is not None or _group(record.name) is not None:
            raise RuntimeError(f"the disposable identity {record.name} survived cleanup")
    state_file.unlink()


def _validate_live_identity(record: AccountRecord) -> None:
    account = _user(record.name)
    group = _group(record.name)
    if not record.created:
        if account is not None or group is not None:
            raise RuntimeError(
                f"the pending account {record.name} has a partial or unowned identity"
            )
        return
    assert record.uid is not None and record.gid is not None
    if account is not None and (
        account.pw_uid != record.uid or account.pw_gid != record.gid
    ):
        raise RuntimeError(f"the disposable account {record.name} changed identity")
    if group is not None and group.gr_gid != record.gid:
        raise RuntimeError(f"the disposable group {record.name} changed identity")


def _record(records: dict[str, AccountRecord], user: str) -> AccountRecord:
    _validate_user(user)
    try:
        return records[user]
    except KeyError as exc:
        raise ValueError(f"{user} is not recorded for this CI-fix run") from exc


def _load(state_file: Path, *, missing_ok: bool = False) -> dict[str, AccountRecord]:
    path = _state_path(state_file)
    try:
        info = path.lstat()
    except FileNotFoundError:
        if missing_ok:
            return {}
        raise ValueError("the CI-fix account state is missing") from None
    if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
        raise ValueError("the CI-fix account state is redirected")
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != _HEADER:
        raise ValueError("the CI-fix account state is malformed")
    records: dict[str, AccountRecord] = {}
    for line in lines[1:]:
        fields = line.split("\t")
        if len(fields) == 2 and fields[0] == "pending":
            record = AccountRecord(fields[1])
        elif len(fields) == 4 and fields[0] == "created":
            try:
                uid, gid = int(fields[2]), int(fields[3])
            except ValueError as exc:
                raise ValueError("the CI-fix account identity is malformed") from exc
            if uid <= 0 or gid <= 0:
                raise ValueError("the CI-fix account identity is malformed")
            record = AccountRecord(fields[1], uid, gid)
        else:
            raise ValueError("the CI-fix account state is malformed")
        _validate_user(record.name)
        if record.name in records:
            raise ValueError("the CI-fix account state contains duplicate records")
        records[record.name] = record
    return records


def _store(state_file: Path, records: dict[str, AccountRecord]) -> None:
    path = _state_path(state_file)
    if path.exists() or path.is_symlink():
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise ValueError("the CI-fix account state is redirected")
    lines = [_HEADER]
    for name in sorted(records):
        record = records[name]
        if record.created:
            lines.append(f"created\t{name}\t{record.uid}\t{record.gid}")
        else:
            lines.append(f"pending\t{name}")
    payload = "\n".join((*lines, ""))
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        fd = -1
        os.replace(temporary_path, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        if fd >= 0:
            os.close(fd)
        temporary_path.unlink(missing_ok=True)
        raise


def _state_path(path: Path) -> Path:
    if not path.is_absolute() or "\n" in str(path):
        raise ValueError("the CI-fix account state path is invalid")
    parent = path.parent.resolve(strict=True)
    if path.parent != parent:
        raise ValueError("the CI-fix account state parent is redirected")
    return path


def _validate_user(user: str) -> None:
    if user not in _USERS:
        raise ValueError("the CI-fix account name is not recognized")


def _user(name: str) -> pwd.struct_passwd | None:
    try:
        return pwd.getpwnam(name)
    except KeyError:
        return None


def _group(name: str) -> grp.struct_group | None:
    try:
        return grp.getgrnam(name)
    except KeyError:
        return None


def _delete_user(name: str) -> None:
    if sys.platform == "darwin":
        _run_root("/usr/bin/dscl", ".", "-delete", f"/Users/{name}")
    else:
        _run_root(_program("userdel"), "-f", name)


def _delete_group(name: str) -> None:
    if sys.platform == "darwin":
        _run_root("/usr/bin/dscl", ".", "-delete", f"/Groups/{name}")
    else:
        _run_root(_program("groupdel"), name)


def _next_darwin_identity() -> int:
    values: list[int] = []
    for node, attribute in (("/Users", "UniqueID"), ("/Groups", "PrimaryGroupID")):
        result = subprocess.run(
            ("/usr/bin/dscl", ".", "-list", node, attribute),
            capture_output=True,
            text=True,
            check=False,
            env=_CLEAN_SYSTEM_ENV,
        )
        if result.returncode:
            raise RuntimeError("could not inspect macOS account identities")
        for line in result.stdout.splitlines():
            fields = line.split()
            if len(fields) != 2 or not fields[1].isdigit():
                raise RuntimeError("macOS returned a malformed account identity")
            values.append(int(fields[1]))
    return max(values, default=500) + 1


def _has_subordinate_ids(user: str) -> bool:
    prefix = f"{user}:"
    for path in (Path("/etc/subuid"), Path("/etc/subgid")):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except FileNotFoundError:
            continue
        if any(line.startswith(prefix) for line in lines):
            return True
    return False


def _program(name: str) -> str:
    program = shutil.which(name, path=_CLEAN_SYSTEM_ENV["PATH"])
    if not program:
        raise RuntimeError(f"the trusted {name} program is unavailable")
    return program


def _run_root(*argv: str) -> None:
    result = subprocess.run(
        [*_root(), *argv],
        capture_output=True,
        text=True,
        check=False,
        env=_CLEAN_SYSTEM_ENV,
    )
    if result.returncode:
        raise RuntimeError(
            f"a disposable CI-fix identity command failed: {result.stderr.strip()}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for command in ("create", "pending", "created", "quiesce"):
        subcommand = commands.add_parser(command)
        subcommand.add_argument("--state-file", type=Path, required=True)
        subcommand.add_argument("--user", choices=sorted(_USERS), required=True)
    cleanup = commands.add_parser("cleanup")
    cleanup.add_argument("--state-file", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "create":
        create_marked(args.state_file, args.user)
    elif args.command == "pending":
        mark_pending(args.state_file, args.user)
    elif args.command == "created":
        mark_created(args.state_file, args.user)
    elif args.command == "quiesce":
        quiesce_marked(args.state_file, args.user)
    else:
        cleanup_marked(args.state_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
