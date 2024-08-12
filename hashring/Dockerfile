# Use the official Rust image as a base
FROM rust:1.70-slim

# Set the working directory inside the container
WORKDIR /usr/src/app

# Copy the Cargo.toml and Cargo.lock files
COPY Cargo.toml Cargo.lock ./

# Copy the source code into the container
COPY . .

# Build the Rust application
RUN cargo build --release

# Expose the port the application runs on
EXPOSE 3000

# Run the Rust application
CMD ["./target/release/swim-consistent-hashing-example"]
