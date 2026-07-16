"""Registry-backed repository allowlist for CI-fix workflows."""

from __future__ import annotations

import argparse

from scripts.backport.registry import Registry, RepoEntry, load_registry
from scripts.common.git_clone import REPO_RE


class CiFixRepositoryError(ValueError):
    """Raised when a repository is not enabled for CI fixing."""


def enabled_ci_fix_repositories(registry: Registry) -> tuple[RepoEntry, ...]:
    """Return repositories explicitly opted into CI fixing, in registry order."""
    return tuple(entry for entry in registry.repos if entry.ci_fix.enabled)


def resolve_ci_fix_repository(registry: Registry, repo_full_name: str) -> RepoEntry:
    """Resolve an explicitly enabled target or reject it."""
    try:
        entry = registry.get_repo(repo_full_name)
    except KeyError as exc:
        raise CiFixRepositoryError(
            f"Repository {repo_full_name!r} is not registered"
        ) from exc
    if not entry.ci_fix.enabled:
        raise CiFixRepositoryError(
            f"Repository {repo_full_name!r} is not enabled for CI fixing"
        )
    return entry


def token_repository_names(
    registry: Registry,
    *,
    owner: str,
    include_repositories: tuple[str, ...] = (),
) -> tuple[str, ...]:
    """Return the least-privilege App token scope for the poller.

    One GitHub App installation token belongs to one owner. Fail if the
    registry enables CI fixing across multiple owners so a repository is never
    silently omitted from polling.
    """
    enabled = enabled_ci_fix_repositories(registry)
    if not enabled:
        raise CiFixRepositoryError("No repositories are enabled for CI fixing")

    wrong_owner = tuple(entry.repo for entry in enabled if _split_repo(entry.repo)[0] != owner)
    if wrong_owner:
        raise CiFixRepositoryError(
            "CI-fix poll targets must share the token owner "
            f"{owner!r}; mismatched repositories: {', '.join(wrong_owner)}"
        )

    full_names = [entry.repo for entry in enabled]
    for repo_full_name in include_repositories:
        repo_owner, _repo_name = _split_repo(repo_full_name)
        if repo_owner != owner:
            raise CiFixRepositoryError(
                f"Included repository {repo_full_name!r} is not owned by {owner!r}"
            )
        full_names.append(repo_full_name)

    names = (_split_repo(repo)[1] for repo in full_names)
    return tuple(dict.fromkeys(names))


def _split_repo(repo_full_name: str) -> tuple[str, str]:
    if not REPO_RE.fullmatch(repo_full_name):
        raise CiFixRepositoryError(
            f"Repository {repo_full_name!r} must have owner/name form"
        )
    owner, name = repo_full_name.split("/", 1)
    return owner, name


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", default="repos.yml")
    subparsers = parser.add_subparsers(dest="command", required=True)

    resolve = subparsers.add_parser(
        "resolve", help="Resolve one enabled repository for a target token"
    )
    resolve.add_argument("--repo", required=True)

    token_scope = subparsers.add_parser(
        "token-scope", help="List repository names for a poller App token"
    )
    token_scope.add_argument("--owner", required=True)
    token_scope.add_argument(
        "--include-repository",
        action="append",
        default=[],
        help="Additional owner/name repository needed by the poller",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        registry = load_registry(args.registry)
        if args.command == "resolve":
            entry = resolve_ci_fix_repository(registry, args.repo)
            owner, name = _split_repo(entry.repo)
            print(f"owner={owner}")
            print(f"name={name}")
            print(f"full_name={entry.repo}")
        else:
            names = token_repository_names(
                registry,
                owner=args.owner,
                include_repositories=tuple(args.include_repository),
            )
            print(f"repositories={','.join(names)}")
    except (CiFixRepositoryError, OSError, ValueError) as exc:
        parser.error(str(exc))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
