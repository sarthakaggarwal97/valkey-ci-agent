FROM python:3.11.13-slim-bookworm@sha256:86adf8dbadc3d6e82ee5dd2c74bec2e1c2467cdad47886280501df722372d2e1

COPY scripts/ai/gateway_proxy.py /opt/gateway/gateway_proxy.py

USER nobody
ENTRYPOINT ["python", "/opt/gateway/gateway_proxy.py"]
