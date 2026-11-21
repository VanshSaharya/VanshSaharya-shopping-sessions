# Build: Rust release binary. Run: a slim image that executes it as uid 1000.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
ENV RUSTFLAGS="-C target-cpu=x86-64-v3"
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/pipeline /app/pipeline
USER 1000
# Keep freed memory in the process (no purge back to the OS): each pipeline phase reuses
# the previous phase's pages instead of faulting in fresh zeroed ones.
ENV MIMALLOC_PURGE_DELAY=-1 MIMALLOC_ALLOW_LARGE_OS_PAGES=1
CMD ["/app/pipeline"]
