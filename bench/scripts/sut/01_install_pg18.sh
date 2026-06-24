#!/usr/bin/env bash
# Install PG18, build deps, Go, and Rust
set -euo pipefail

GO_VERSION="${GO_VERSION:-1.26.2}"
RUST_VERSION="${RUST_VERSION:-1.89.0}"
GO_TARBALL="go${GO_VERSION}.linux-amd64.tar.gz"
GO_URL="https://go.dev/dl/${GO_TARBALL}"
# User owning cargo toolchain
BUILD_USER="${BUILD_USER:-${SUDO_USER:-ubuntu}}"

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must run as root (use sudo)." >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

echo "=== Adding PGDG apt repository ==="
apt-get update -qq
apt-get install -y wget gnupg lsb-release ca-certificates
install -d -m 0755 /usr/share/keyrings
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  | gpg --dearmor --batch --yes -o /usr/share/keyrings/pgdg.gpg
echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
  > /etc/apt/sources.list.d/pgdg.list

echo "=== Installing PostgreSQL 18 and build dependencies ==="
apt-get update -qq
apt-get install -y \
  postgresql-18 \
  postgresql-server-dev-18 \
  build-essential \
  git \
  curl \
  pkg-config \
  libssl-dev \
  liblz4-dev \
  cmake \
  sysstat \
  unzip

echo "=== Enabling sysstat data collection (pidstat) ==="
if [[ -f /etc/default/sysstat ]]; then
  sed -i 's/^ENABLED=.*/ENABLED="true"/' /etc/default/sysstat || true
fi
systemctl enable --now sysstat 2>/dev/null || true

echo "=== Installing AWS CLI v2 (the apt 'awscli' package is gone on noble) ==="
if ! command -v aws >/dev/null 2>&1; then
  tmp="$(mktemp -d)"
  curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "${tmp}/awscliv2.zip"
  unzip -q "${tmp}/awscliv2.zip" -d "${tmp}"
  "${tmp}/aws/install" --update
  rm -rf "${tmp}"
fi
aws --version

echo "=== Installing Go ${GO_VERSION} to /usr/local/go ==="
if [[ ! -x /usr/local/go/bin/go ]] || ! /usr/local/go/bin/go version | grep -q "go${GO_VERSION}"; then
  tmp="$(mktemp -d)"
  curl -fsSL "${GO_URL}" -o "${tmp}/${GO_TARBALL}"
  rm -rf /usr/local/go
  tar -C /usr/local -xzf "${tmp}/${GO_TARBALL}"
  rm -rf "${tmp}"
fi
# Put Go on login-shell PATH
cat > /etc/profile.d/go.sh <<'EOF'
export PATH=$PATH:/usr/local/go/bin
EOF
chmod 0644 /etc/profile.d/go.sh
/usr/local/go/bin/go version

echo "=== Installing rustup + toolchain ${RUST_VERSION} for user ${BUILD_USER} ==="
if ! id "${BUILD_USER}" >/dev/null 2>&1; then
  echo "ERROR: build user '${BUILD_USER}' does not exist." >&2
  exit 1
fi
build_home="$(getent passwd "${BUILD_USER}" | cut -d: -f6)"
if [[ -z "${build_home}" ]]; then
  echo "ERROR: cannot resolve home directory for ${BUILD_USER}." >&2
  exit 1
fi

if [[ ! -x "${build_home}/.cargo/bin/cargo" ]]; then
  sudo -u "${BUILD_USER}" -H bash -c \
    "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
       | sh -s -- -y --default-toolchain ${RUST_VERSION} --profile minimal"
else
  sudo -u "${BUILD_USER}" -H bash -c \
    "${build_home}/.cargo/bin/rustup toolchain install ${RUST_VERSION} --profile minimal && \
     ${build_home}/.cargo/bin/rustup default ${RUST_VERSION}"
fi
sudo -u "${BUILD_USER}" -H bash -c "${build_home}/.cargo/bin/rustc --version"

echo "Done. PostgreSQL 18, Go ${GO_VERSION}, Rust ${RUST_VERSION} installed."
/usr/lib/postgresql/18/bin/pg_config --version
