# Build stage: compile the pipeline for linux/amd64.
FROM rust:1-slim-bookworm AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked && strip target/release/pipeline

# Runtime stage: just the binary.
FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /build/target/release/pipeline /app/pipeline
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh
USER 1000
WORKDIR /app
CMD ["/app/run.sh"]
