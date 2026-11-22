# Build stage: compile the pipeline for linux/amd64.
# The compiler is pinned to the exact release that recorded pgo/merged.profdata,
# a profile of the pipeline on generated data, so the profile matches the code.
FROM rust:1.100.0-slim-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY pgo ./pgo
ARG PIPELINE_CPU=x86-64-v3
RUN cargo rustc --release --locked -- -C "target-cpu=${PIPELINE_CPU}" \
        -C profile-use=/build/pgo/merged.profdata \
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
