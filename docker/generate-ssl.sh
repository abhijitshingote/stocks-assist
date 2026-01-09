#!/bin/bash
# Generate self-signed SSL certificates for development/local use
# Run this script once to create the certificates

SSL_DIR="$(dirname "$0")/ssl"
mkdir -p "$SSL_DIR"

# Check if certificates already exist
if [ -f "$SSL_DIR/cert.pem" ] && [ -f "$SSL_DIR/key.pem" ]; then
    echo "SSL certificates already exist in $SSL_DIR"
    echo "Delete them first if you want to regenerate."
    exit 0
fi

echo "Generating self-signed SSL certificates..."

openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout "$SSL_DIR/key.pem" \
    -out "$SSL_DIR/cert.pem" \
    -subj "/C=US/ST=State/L=City/O=Organization/CN=localhost" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

if [ $? -eq 0 ]; then
    echo "✅ SSL certificates generated successfully!"
    echo "   Certificate: $SSL_DIR/cert.pem"
    echo "   Private Key: $SSL_DIR/key.pem"
    echo ""
    echo "Note: These are self-signed certificates for development."
    echo "Your browser will show a security warning - this is expected."
else
    echo "❌ Failed to generate SSL certificates"
    exit 1
fi

