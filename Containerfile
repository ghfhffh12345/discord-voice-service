FROM docker.io/library/rust:1.89-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --locked -p discord-voice-service

FROM gcr.io/distroless/cc-debian12
WORKDIR /app
COPY --from=builder /app/target/release/discord-voice-service /app/discord-voice-service
USER nonroot:nonroot
EXPOSE 55051
ENTRYPOINT ["/app/discord-voice-service"]
