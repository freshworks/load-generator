FROM debian:bookworm-slim

RUN \
  apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    procps \
    dnsutils \
    netcat-openbsd \
    net-tools \
    git \
    vim \
  && rm -rf /var/lib/apt/lists/*

USER nobody:nobody
COPY lg /opt/
COPY scripts/ /opt/scripts/
