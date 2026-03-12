FROM rust:1.93 AS builder

WORKDIR /app

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY api ./api

RUN cargo build --release --package mylib-api

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
	ca-certificates \
	libssl3 \
	curl \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/mylib-api .

ENV LISTEN_ADDR=0.0.0.0:3000
ENV RUST_LOG=info

EXPOSE 3000

CMD ["./mylib-api"]
