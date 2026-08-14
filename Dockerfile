FROM rust:1.94-bookworm AS builder

USER root
RUN apt-get update
RUN apt-get install -y apt-utils
RUN apt-get install -y --no-install-recommends clang openssl libssl-dev pkg-config

RUN apt-get update \
    && apt-get -y install --no-install-recommends curl ca-certificates xz-utils build-essential clang cmake pkg-config libjpeg-turbo-progs libpng-dev \
    && rm -rfv /var/lib/apt/lists/*

# FFmpeg 8 for the `ffmpeg` video-thumbnail feature, installed via the shared helper. Pins live in
# tool-versions.env, the single source of truth shared with Linux dev (bin/install-prereqs) and CI.
ARG TARGETARCH
COPY bin/install-ffmpeg tool-versions.env /tmp/ffmpeg-install/
RUN FFMPEG_ARCH="$TARGETARCH" TOOL_VERSIONS_FILE=/tmp/ffmpeg-install/tool-versions.env \
    bash /tmp/ffmpeg-install/install-ffmpeg \
    && rm -rf /tmp/ffmpeg-install
ENV PKG_CONFIG_PATH="/opt/ffmpeg/lib/pkgconfig:${PKG_CONFIG_PATH:-}"

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

### This is breaking because cargo-build-deps forces an update to dependencies
### Commenting out and using a simpler approach until we find an alternative
###
# RUN cargo install cargo-build-deps

# # create an empty project to install dependencies
# RUN cd /usr/src && cargo new --bin oxen-server
# WORKDIR /usr/src/oxen-server
# COPY Cargo.toml Cargo.lock ./
# COPY crates/liboxen/Cargo.toml crates/liboxen/Cargo.toml
# COPY crates/oxen-cli/Cargo.toml crates/oxen-cli/Cargo.toml
# COPY crates/oxen-server/Cargo.toml crates/oxen-server/Cargo.toml
# # build just the deps for caching
# RUN cargo build-deps --release

# # copy the rest of the source and build the server and cli
# COPY src src
# RUN cargo build --release
### end commented section

WORKDIR /usr/src/oxen-server
COPY . .

# Defaults to what `[profile.release]` in Cargo.toml already sets, so a build passing
# no override compiles exactly what a plain release build compiles. Cargo reads this
# as a profile override, the same mechanism release_windows.yml uses to select thin
# LTO. It is re-declared as `ENV` because an `ARG` alone is not reliably present in
# the `RUN` process environment.
ARG CARGO_PROFILE_RELEASE_LTO=true
ENV CARGO_PROFILE_RELEASE_LTO=${CARGO_PROFILE_RELEASE_LTO}

# Keeps the symbol table in the server binary, so a panic reported to Sentry names its frames.
# `debuginfo` drops DWARF, so a frame carries a function name but no file or line. Scoped to this
# image: every other build of the workspace uses the `strip` setting in `[profile.release]`.
ARG CARGO_PROFILE_RELEASE_STRIP=debuginfo
ENV CARGO_PROFILE_RELEASE_STRIP=${CARGO_PROFILE_RELEASE_STRIP}

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
