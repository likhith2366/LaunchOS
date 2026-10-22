# Multi-stage: build with the full Swift toolchain, ship on the slim runtime image.
# The runtime image has no compiler, so the thing that gets deployed is a few hundred
# megabytes rather than five gigabytes.

FROM swift:6.2-noble AS build

WORKDIR /build

# Dependencies first, in their own layer. Package.swift and Package.resolved change far
# less often than the source does, so a source-only edit reuses the resolved checkouts
# instead of fetching every dependency again.
COPY Package.swift Package.resolved ./
RUN swift package resolve

COPY Sources ./Sources
COPY Tests ./Tests

RUN swift build -c release --product launch-server \
    && swift build -c release --product launch-sim

# Collect the binaries and the Swift runtime libraries they need.
RUN mkdir -p /out/bin \
    && cp "$(swift build -c release --show-bin-path)/launch-server" /out/bin/ \
    && cp "$(swift build -c release --show-bin-path)/launch-sim" /out/bin/

FROM swift:6.2-noble-slim AS runtime

# Runs unprivileged. A launch service has no business being root, and the one thing it
# writes to the filesystem is logs on stdout.
RUN groupadd --gid 1001 launchos \
    && useradd --uid 1001 --gid launchos --create-home --shell /usr/sbin/nologin launchos \
    && apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /out/bin/launch-server /usr/local/bin/launch-server
COPY --from=build /out/bin/launch-sim /usr/local/bin/launch-sim

USER launchos
WORKDIR /home/launchos

EXPOSE 8080

# The node answers /health as soon as the schema is in place and it is ready to take
# traffic, so Compose can hold dependents back until it genuinely is.
HEALTHCHECK --interval=5s --timeout=3s --start-period=20s --retries=10 \
    CMD curl -fsS http://127.0.0.1:${PORT:-8080}/health || exit 1

ENTRYPOINT ["launch-server"]
CMD ["serve", "--hostname", "0.0.0.0", "--port", "8080"]
