# Use multi-stage build for smaller final image
# Build stage
FROM rust:1.88-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libpq-dev \
    build-essential \
    clang \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy Cargo files and source code
COPY Cargo.toml Cargo.lock ./
COPY pg2any-lib/ ./pg2any-lib/
COPY examples/ ./examples/

WORKDIR /app/examples
# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    ca-certificates \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create non-root user
RUN useradd -r -u 1001 -g root pg2any_user

# Copy the binary from builder stage
COPY --from=builder /app/target/release/pg2any /usr/local/bin/pg2any

# Change ownership to non-root user
RUN chown -R pg2any_user:root /app

# Switch to non-root user
USER pg2any_user

# Expose port (if needed for health checks or metrics)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD pgrep -f pg2any || exit 1

# Run the application
CMD ["pg2any"]
