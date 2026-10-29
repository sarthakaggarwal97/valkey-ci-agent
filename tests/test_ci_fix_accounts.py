from types import SimpleNamespace

import pytest

from scripts.ci_fix import accounts


def _identity(uid=501, gid=502):
    return (
        SimpleNamespace(pw_uid=uid, pw_gid=gid),
        SimpleNamespace(gr_gid=gid),
    )


def _creation_spy(tmp_path, monkeypatch, platform, *, subordinate_ids=False):
    state = tmp_path / "accounts"
    events = []
    monkeypatch.setattr(accounts.sys, "platform", platform)
    monkeypatch.setattr(
        accounts,
        "mark_pending",
        lambda path, user: events.append(("pending", path, user)),
    )
    monkeypatch.setattr(accounts, "_program", lambda name: f"/trusted/{name}")
    monkeypatch.setattr(accounts, "_next_darwin_identity", lambda: 612)
    monkeypatch.setattr(
        accounts,
        "_run_root",
        lambda *argv: events.append(("create", argv)),
    )

    def has_subordinate_ids(user):
        events.append(("subordinate-ids", user))
        return subordinate_ids

    monkeypatch.setattr(accounts, "_has_subordinate_ids", has_subordinate_ids)
    monkeypatch.setattr(
        accounts,
        "mark_created",
        lambda path, user: events.append(("created", path, user)),
    )
    return state, events


def test_linux_creation_uses_a_system_account_and_checks_subordinate_ids(
    tmp_path,
    monkeypatch,
):
    state, events = _creation_spy(tmp_path, monkeypatch, "linux")

    accounts.create_marked(state, "ci-fix-agent")

    assert events == [
        ("pending", state, "ci-fix-agent"),
        (
            "create",
            (
                "/trusted/useradd",
                "--system",
                "--user-group",
                "--no-create-home",
                "--home-dir",
                "/nonexistent",
                "--shell",
                "/bin/bash",
                "ci-fix-agent",
            ),
        ),
        ("subordinate-ids", "ci-fix-agent"),
        ("created", state, "ci-fix-agent"),
    ]


def test_linux_creation_refuses_an_account_with_subordinate_ids(tmp_path, monkeypatch):
    state, events = _creation_spy(
        tmp_path,
        monkeypatch,
        "linux",
        subordinate_ids=True,
    )

    with pytest.raises(RuntimeError, match="received subordinate IDs"):
        accounts.create_marked(state, "ci-fix-agent")

    assert [event[0] for event in events] == [
        "pending",
        "create",
        "subordinate-ids",
    ]


def test_macos_creation_uses_one_dscl_identity_before_marking_created(
    tmp_path,
    monkeypatch,
):
    state, events = _creation_spy(tmp_path, monkeypatch, "darwin")

    accounts.create_marked(state, "ci-fix-agent")

    commands = [event[1] for event in events if event[0] == "create"]
    assert commands == [
        ("/usr/bin/dscl", ".", "-create", "/Groups/ci-fix-agent", "PrimaryGroupID", "612"),
        ("/usr/bin/dscl", ".", "-create", "/Users/ci-fix-agent"),
        ("/usr/bin/dscl", ".", "-create", "/Users/ci-fix-agent", "UniqueID", "612"),
        ("/usr/bin/dscl", ".", "-create", "/Users/ci-fix-agent", "PrimaryGroupID", "612"),
        ("/usr/bin/dscl", ".", "-create", "/Users/ci-fix-agent", "UserShell", "/bin/bash"),
        (
            "/usr/bin/dscl",
            ".",
            "-create",
            "/Users/ci-fix-agent",
            "NFSHomeDirectory",
            "/Users/ci-fix-agent",
        ),
    ]
    assert events[0] == ("pending", state, "ci-fix-agent")
    assert events[-1] == ("created", state, "ci-fix-agent")


@pytest.mark.parametrize("existing", ["user", "group"])
def test_pending_marker_rejects_a_preexisting_fixed_name(tmp_path, monkeypatch, existing):
    user, group = _identity()
    monkeypatch.setattr(accounts, "_user", lambda _name: user if existing == "user" else None)
    monkeypatch.setattr(accounts, "_group", lambda _name: group if existing == "group" else None)

    with pytest.raises(RuntimeError, match="already in use"):
        accounts.mark_pending(tmp_path / "accounts", "ci-fix-agent")

    assert not (tmp_path / "accounts").exists()


@pytest.mark.parametrize("unsafe", ["pending-user", "changed-user", "changed-group"])
def test_cleanup_never_deletes_an_unproven_identity(tmp_path, monkeypatch, unsafe):
    state = tmp_path / "accounts"
    live_user = None
    live_group = None
    monkeypatch.setattr(accounts, "_user", lambda _name: live_user)
    monkeypatch.setattr(accounts, "_group", lambda _name: live_group)
    accounts.mark_pending(state, "ci-fix-agent")
    user, group = _identity()
    if unsafe != "pending-user":
        live_user, live_group = user, group
        accounts.mark_created(state, "ci-fix-agent")
    if unsafe == "pending-user":
        live_user = user
    elif unsafe == "changed-user":
        live_user = SimpleNamespace(pw_uid=999, pw_gid=group.gr_gid)
    else:
        live_group = SimpleNamespace(gr_gid=999)

    touched = []
    monkeypatch.setattr(accounts, "quiesce_account", lambda *_args, **_kwargs: touched.append("quiesce"))
    monkeypatch.setattr(accounts, "_delete_user", lambda _name: touched.append("user"))
    monkeypatch.setattr(accounts, "_delete_group", lambda _name: touched.append("group"))

    with pytest.raises(RuntimeError, match="pending|changed identity"):
        accounts.cleanup_marked(state)

    assert touched == []
    assert state.exists()


def test_completed_identity_is_quiesced_then_removed(tmp_path, monkeypatch):
    state = tmp_path / "accounts"
    live = {"user": None, "group": None}
    monkeypatch.setattr(accounts, "_user", lambda _name: live["user"])
    monkeypatch.setattr(accounts, "_group", lambda _name: live["group"])
    accounts.mark_pending(state, "ci-fix-agent")
    live["user"], live["group"] = _identity()
    accounts.mark_created(state, "ci-fix-agent")

    events = []
    monkeypatch.setattr(
        accounts,
        "quiesce_account",
        lambda name, expected_uid: events.append(("quiesce", name, expected_uid)),
    )

    def delete_user(name):
        events.append(("user", name))
        live["user"] = None

    def delete_group(name):
        events.append(("group", name))
        live["group"] = None

    monkeypatch.setattr(accounts, "_delete_user", delete_user)
    monkeypatch.setattr(accounts, "_delete_group", delete_group)

    accounts.cleanup_marked(state)

    assert events == [
        ("quiesce", "ci-fix-agent", 501),
        ("user", "ci-fix-agent"),
        ("group", "ci-fix-agent"),
    ]
    assert not state.exists()
