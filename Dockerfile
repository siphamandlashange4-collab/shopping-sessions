# Build stage: compile the pipeline for linux/amd64.
FROM rust:1-slim-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ARG PIPELINE_CPU=x86-64-v3
RUN apt-get update -qq \
    && apt-get install -qy --no-install-recommends curl unzip \
    && rustup component add llvm-tools-preview

# Train the compiler on generated inputs. The runtime stage copies only the
# optimized executable.
RUN curl --fail --silent --show-error --location \
        https://www.joinbuilders.org/buildathon/tracks/shopping-sessions/files/generator.zip \
        --output /tmp/generator.zip \
    && echo '13724cadbac5a99d847e27cea911cd2e338ff972b46c2039da87452011bb101e  /tmp/generator.zip' \
        | sha256sum -c - \
    && unzip -q /tmp/generator.zip -d /tmp/training \
    && cargo build --release --locked --manifest-path /tmp/training/generator/Cargo.toml

RUN cargo rustc --release --locked -- \
        -C "target-cpu=${PIPELINE_CPU}" -C profile-generate=/tmp/profiles \
    && for seed in 107 109; do \
        /tmp/training/generator/target/release/generate --threads 4 --seed "$seed" \
            --events 2000000 --clicks 500000 --output "/tmp/training/data-$seed" \
        && NCPU=4 DATA_DIR="/tmp/training/data-$seed" OUTPUT_DIR="/tmp/training/out-$seed" \
            LLVM_PROFILE_FILE="/tmp/profiles/run-%m-%p.profraw" target/release/pipeline \
        || exit 1; \
    done \
    && "$(rustc --print target-libdir)/../bin/llvm-profdata" merge \
        -o /tmp/merged.profdata /tmp/profiles \
    && cargo rustc --release --locked -- \
        -C "target-cpu=${PIPELINE_CPU}" -C profile-use=/tmp/merged.profdata \
    && strip target/release/pipeline

# Runtime stage: just the binary.
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /build/target/release/pipeline /app/pipeline
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh
USER 1000
WORKDIR /app
CMD ["/app/run.sh"]
