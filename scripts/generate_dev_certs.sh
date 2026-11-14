#!/usr/bin/env bash
set -euo pipefail

CERT_DIR="${1:-./certs}"
mkdir -p "$CERT_DIR"
umask 077

echo "Generating self-signed CA and server TLS certificates in $CERT_DIR ..."

# 1. Generate CA key and certificate
openssl req -x509 -newkey rsa:4096 -days 3650 -nodes \
  -keyout "$CERT_DIR/ca.key" \
  -out "$CERT_DIR/ca.crt" \
  -subj "/CN=as-proxy-ca"

# 2. Generate Server key and CSR
openssl req -newkey rsa:4096 -nodes \
  -keyout "$CERT_DIR/server.key" \
  -out "$CERT_DIR/server.csr" \
  -subj "/CN=localhost"

# 3. Sign Server certificate with SANs for localhost and 127.0.0.1
cat > "$CERT_DIR/server_ext.cnf" <<EOF
subjectAltName = DNS:localhost,IP:127.0.0.1
extendedKeyUsage = serverAuth
EOF

openssl x509 -req -in "$CERT_DIR/server.csr" \
  -CA "$CERT_DIR/ca.crt" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
  -out "$CERT_DIR/server.crt" -days 3650 \
  -extfile "$CERT_DIR/server_ext.cnf"

rm -f "$CERT_DIR/server.csr" "$CERT_DIR/server_ext.cnf" "$CERT_DIR/ca.srl"
chmod 600 "$CERT_DIR/ca.key" "$CERT_DIR/server.key"
chmod 644 "$CERT_DIR/ca.crt" "$CERT_DIR/server.crt"
echo "Done! Generated: $CERT_DIR/ca.crt, $CERT_DIR/server.crt, $CERT_DIR/server.key"
