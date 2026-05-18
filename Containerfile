FROM docker.io/library/rust:1.88-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --release

FROM gcr.io/distroless/cc-debian12
WORKDIR /app
COPY --from=builder /app/target/release/discord-voice-service /app/discord-voice-service
USER nonroot:nonroot
EXPOSE 55051
ENTRYPOINT ["/app/discord-voice-service"]
