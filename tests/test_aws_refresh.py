from __future__ import annotations

import io
import json
import os
import urllib.parse

import pytest

from scripts.ai import aws_refresh

STS_REPLY = b"""<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult><Credentials>
    <AccessKeyId>AKIDNEW</AccessKeyId><SecretAccessKey>secret-new</SecretAccessKey>
    <SessionToken>token-new</SessionToken><Expiration>2026-11-04T17:00:00Z</Expiration>
  </Credentials></AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>"""


@pytest.fixture
def job(monkeypatch):
    monkeypatch.setattr(aws_refresh, "_refreshed_at", None)
    monkeypatch.setenv("CI_AGENT_AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/ci")
    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_URL", "https://token.example/id?api-version=2.0")
    monkeypatch.setenv("ACTIONS_ID_TOKEN_REQUEST_TOKEN", "request-token")
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(name, "old")
    requests = []

    def urlopen(request, timeout):
        requests.append(request)
        if request.full_url.startswith("https://token.example/"):
            return io.BytesIO(json.dumps({"value": "oidc-jwt"}).encode())
        return io.BytesIO(STS_REPLY)

    monkeypatch.setattr(aws_refresh.urllib.request, "urlopen", urlopen)
    return requests


def test_assumes_the_role_again_with_the_jobs_oidc_token(job):
    aws_refresh.refresh_aws_credentials()

    token_request, sts_request = job
    assert token_request.full_url == "https://token.example/id?api-version=2.0&audience=sts.amazonaws.com"
    assert token_request.get_header("Authorization") == "bearer request-token"
    assert sts_request.full_url == "https://sts.us-west-2.amazonaws.com/"
    form = urllib.parse.parse_qs(sts_request.data.decode())
    assert form["Action"] == ["AssumeRoleWithWebIdentity"] and form["WebIdentityToken"] == ["oidc-jwt"]
    assert form["RoleArn"] == ["arn:aws:iam::123456789012:role/ci"] and form["DurationSeconds"] == ["3600"]
    assert form["RoleSessionName"] == ["ci-agent-42"]
    assert (os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"], os.environ["AWS_SESSION_TOKEN"]) == (
        "AKIDNEW", "secret-new", "token-new")


def test_refreshes_at_most_every_ten_minutes(job, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(aws_refresh.time, "monotonic", lambda: now[0])
    aws_refresh.refresh_aws_credentials()
    now[0] += 9 * 60
    aws_refresh.refresh_aws_credentials()
    assert len(job) == 2
    now[0] += 2 * 60
    aws_refresh.refresh_aws_credentials()
    assert len(job) == 4


def test_does_nothing_outside_a_job_that_names_the_role(job, monkeypatch):
    monkeypatch.delenv("CI_AGENT_AWS_ROLE_ARN")
    aws_refresh.refresh_aws_credentials()
    assert job == [] and os.environ["AWS_ACCESS_KEY_ID"] == "old"


def test_a_failed_refresh_keeps_the_current_credentials(job, monkeypatch):
    def refuse(request, timeout):
        raise OSError("403 AccessDenied")

    monkeypatch.setattr(aws_refresh.urllib.request, "urlopen", refuse)
    aws_refresh.refresh_aws_credentials()
    assert os.environ["AWS_ACCESS_KEY_ID"] == "old" and aws_refresh._refreshed_at is None
