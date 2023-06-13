FROM rust:1.70.0 as builder

USER root
RUN apt-get update
RUN apt-get install -y apt-utils
RUN apt-get install -y libclang-dev libavutil-dev libavformat-dev libavdevice-dev libavfilter-dev libswscale-dev libswresample-dev libpostproc-dev libssl-dev pkg-config

RUN apt-get update \
 && apt-get -y install curl build-essential clang cmake pkg-config libjpeg-turbo-progs libpng-dev \
 && rm -rfv /var/lib/apt/lists/*

ENV MAGICK_VERSION 7.1

# RUN curl https://imagemagick.org/archive/ImageMagick.tar.gz | tar xz \
#  && cd ImageMagick-${MAGICK_VERSION}* \
#  && ./configure --with-magick-plus-plus=no --with-perl=no \
#  && make \
#  && make install \
#  && cd .. \
#  && rm -r ImageMagick-${MAGICK_VERSION}*

RUN cargo install cargo-build-deps

# create an empty project to install dependencies
RUN cd /usr/src && cargo new --bin oxen-server
WORKDIR /usr/src/oxen-server
COPY Cargo.toml Cargo.lock ./
COPY src/lib/Cargo.toml src/lib/Cargo.toml
COPY src/cli/Cargo.toml src/cli/Cargo.toml
COPY src/server/Cargo.toml src/server/Cargo.toml
# build just the deps for caching
RUN cargo build-deps --release

# copy the rest of the source and build the server
COPY src src
RUN cargo build --release --bin oxen-server

# Minimal image to run the binary (without Rust toolchain)
FROM debian:bullseye-slim AS runtime
WORKDIR /oxen-server
COPY --from=builder /usr/src/oxen-server/target/release/oxen-server /usr/local/bin
ENV SYNC_DIR=/var/oxen/data
EXPOSE 3001
CMD ["oxen-server", "start", "-p", "3001"]