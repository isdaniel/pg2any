# Use multi-stage build for smaller final image
# Build stage
FROM rust:1.82-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy Cargo files first to leverage Docker layer caching
COPY Cargo.toml Cargo.lock ./

# Create a dummy main.rs to build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs

# Build dependencies (this layer will be cached unless Cargo.toml changes)
RUN cargo build --release && rm src/main.rs target/release/deps/pg2any*

# Copy source code
COPY src ./src

# Build the actual application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libpq5 \
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

# Set up environment variables with defaults
ENV RUST_LOG=info
ENV CDC_SOURCE_HOST=postgres
ENV CDC_SOURCE_PORT=5432
ENV CDC_SOURCE_DB=postgres
ENV CDC_SOURCE_USER=postgres
ENV CDC_SOURCE_PASSWORD=test.123
ENV CDC_DEST_HOST=mysql
ENV CDC_DEST_PORT=3306
ENV CDC_DEST_DB=cdc_db
ENV CDC_DEST_USER=cdc_user
ENV CDC_DEST_PASSWORD=test.123

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD pgrep -f pg2any || exit 1

# Run the application
CMD ["pg2any"]
