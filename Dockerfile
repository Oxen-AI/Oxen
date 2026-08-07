# Toolchain and system dependencies, shared by every stage that compiles anything.
FROM rust:1.94-bookworm AS chef

USER root
RUN apt-get update
RUN apt-get install -y apt-utils
RUN apt-get install -y --no-install-recommends clang openssl libssl-dev pkg-config

RUN apt-get update \
    && apt-get -y install --no-install-recommends curl ca-certificates xz-utils build-essential clang cmake pkg-config libjpeg-turbo-progs libpng-dev \
    && rm -rfv /var/lib/apt/lists/*

# ENV MAGICK_VERSION 7.1

# RUN curl https://imagemagick.org/archive/ImageMagick.tar.gz | tar xz \
#  && cd ImageMagick-${MAGICK_VERSION}* \
#  && ./configure --with-magick-plus-plus=no --with-perl=no \
#  && make \
#  && make install \
#  && cd .. \
#  && rm -r ImageMagick-${MAGICK_VERSION}*

# RUN git clone https://github.com/rui314/mold.git \
#     && mkdir mold/build \
#     && cd mold/build \
#     && git checkout v2.0.0 \
#     && ../install-build-deps.sh \
#     && cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=c++ .. \
#     && cmake --build . -j $(nproc) \
#     && cmake --install .

# FFmpeg 8 for the `ffmpeg` video-thumbnail feature, installed via the shared helper. Pins live in
# tool-versions.env, the single source of truth shared with Linux dev (bin/install-prereqs) and CI.
# That file stays for the cargo-chef install below; only the helper script is cleaned up.
ARG TARGETARCH
COPY bin/install-ffmpeg tool-versions.env /tmp/
RUN FFMPEG_ARCH="$TARGETARCH" TOOL_VERSIONS_FILE=/tmp/tool-versions.env \
    bash /tmp/install-ffmpeg \
    && rm -f /tmp/install-ffmpeg
ENV PKG_CONFIG_PATH="/opt/ffmpeg/lib/pkgconfig:${PKG_CONFIG_PATH:-}"

# cargo-chef keys the dependency build on the manifests instead of the source tree.
# Version pinned in tool-versions.env, as with the FFmpeg install above.
RUN . /tmp/tool-versions.env \
    && cargo install cargo-chef --locked --version "$CARGO_CHEF_VERSION"

WORKDIR /usr/src/oxen-server

# recipe.json describes the dependency graph from the manifests alone. A source edit
# that leaves them untouched produces an identical recipe, so the cook layer survives.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

# Defaults to what `[profile.release]` in Cargo.toml already sets, so a build passing
# no override compiles exactly what a plain release build compiles. Cargo reads this
# as a profile override, the same mechanism release_windows.yml uses to select thin
# LTO. It is re-declared as `ENV` because an `ARG` alone is not reliably present in
# the `RUN` process environment.
#
# Must precede cook: the profile a dependency compiled under is part of its cached
# artifact, so cooking and building under different profiles discards cook's work.
ARG CARGO_PROFILE_RELEASE_LTO=true
ENV CARGO_PROFILE_RELEASE_LTO=${CARGO_PROFILE_RELEASE_LTO}

# Keeps the symbol table in the server binary, so a panic reported to Sentry names its frames.
# `debuginfo` drops DWARF, so a frame carries a function name but no file or line. Scoped to this
# image: every other build of the workspace uses the `strip` setting in `[profile.release]`.
# Also a profile setting, so it precedes cook for the reason the LTO override above gives.
ARG CARGO_PROFILE_RELEASE_STRIP=debuginfo
ENV CARGO_PROFILE_RELEASE_STRIP=${CARGO_PROFILE_RELEASE_STRIP}

# cook compiles dependencies only. Its flags must match the build below, features
# included, or feature unification differs and the dependencies compile again instead
# of being reused. `-p` stands in for `--workspace --exclude oxen-py`, which cook
# cannot express: oxen-py needs a Python toolchain this image lacks, and oxen-cli plus
# oxen-server reach the same dependencies.
COPY --from=planner /usr/src/oxen-server/recipe.json recipe.json
RUN cargo chef cook --release --features liboxen/ffmpeg,oxen-server/otel \
    -p oxen-cli -p oxen-server --recipe-path recipe.json

COPY . .
# `oxen-server/otel` compiles the OTLP span exporter and inbound W3C trace-context extraction into
# the binary; both stay dormant until an OTLP endpoint is configured at runtime. Named explicitly
# rather than via the `production` feature, which additionally turns on `perf-logging`.
RUN cargo build --workspace --exclude oxen-py --release --features liboxen/ffmpeg,oxen-server/otel

# The CLI ships without a symbol table. Only oxen-server reports panics.
RUN strip target/release/oxen

# Minimal image to run the binary (without Rust toolchain)
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends openssl curl ca-certificates \
    && rm -rfv /var/lib/apt/lists/*

# FFmpeg 8 shared libraries for the `ffmpeg` video-thumbnail feature (see builder stage).
COPY --from=builder /opt/ffmpeg/lib /opt/ffmpeg/lib
RUN echo /opt/ffmpeg/lib > /etc/ld.so.conf.d/ffmpeg.conf && ldconfig

WORKDIR /oxen-server
COPY --from=builder /usr/src/oxen-server/target/release/oxen /usr/local/bin
COPY --from=builder /usr/src/oxen-server/target/release/oxen-server /usr/local/bin
# 50MB stack size (should be more than enough...)
ENV RUST_MIN_STACK=50000000
# Set the log level to info for the server
ENV RUST_LOG=info
ENV SYNC_DIR=/var/oxen/data
EXPOSE 3001
CMD ["oxen-server", "start", "-p", "3001"]
