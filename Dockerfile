FROM rust:alpine AS builder

# Dependencies for some crates if needed
# RUN apk add --no-cache alpine-sdk cmake

WORKDIR /usr/src/app

# First build a dummy target to cache dependencies in a separate Docker layer
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() { println!("Dummy image called!"); }' > src/main.rs
# Silence warning about every dependecy being unused
RUN CARGO_BUILD_WARNINGS=allow cargo build --release

# Now build the real target
COPY src ./src
# Update modified attribute as otherwise cargo won't rebuild it
RUN touch -a -m ./src/main.rs
RUN cargo build --release

FROM alpine AS runtime
COPY --from=builder /usr/src/app/target/release/rati /usr/local/bin/rati
CMD ["rati"]
