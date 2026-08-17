FROM postgres:16

# The official image doesn't ship the wal2json output plugin our WAL reader depends on for
# readable JSON change payloads (vs. binary pgoutput/decoderbufs), but its apt repo carries it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-16-wal2json \
    && rm -rf /var/lib/apt/lists/*
