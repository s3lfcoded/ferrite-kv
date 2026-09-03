# Build stage
FROM rust:alpine AS builder
WORKDIR /app
RUN apk add --no-cache musl-dev

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked

# Runtime stage
FROM alpine:3.20
WORKDIR /app

RUN addgroup -S ferrite && adduser -S ferrite -G ferrite
USER ferrite

COPY --from=builder /app/target/release/ferrite-kv /usr/local/bin/ferrite-kv

EXPOSE 6379

ENTRYPOINT ["ferrite-kv"]
CMD ["--port", "6379"]
