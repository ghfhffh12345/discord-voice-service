FROM rust:1.88-bookworm AS build
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /app/target/release/discord-voice-service /usr/local/bin/discord-voice-service
CMD ["discord-voice-service"]
