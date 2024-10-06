FROM rust:1.81.0 AS build

RUN apt update && apt install -y mold

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN mold --run cargo build --release

FROM gcr.io/distroless/cc

WORKDIR /app
COPY --from=build /app/target/release/rnet .

WORKDIR /data
ENTRYPOINT ["/app/rnet"]
CMD ["--help"]
