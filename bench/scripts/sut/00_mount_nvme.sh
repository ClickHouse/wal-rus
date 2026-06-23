#!/usr/bin/env bash
# Detect the non-root NVMe instance-store device, format it ext4 (only if not
# already formatted), mount it at /dat, and prepare the PG18 data parent dir.
set -euo pipefail

MOUNT_POINT=/dat
PG_PARENT="${MOUNT_POINT}/18"

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must run as root (use sudo)." >&2
  exit 1
fi

# The root filesystem's backing device. lsblk -no PKNAME gives the parent disk
# of whatever device hosts "/", e.g. nvme0n1.
root_src="$(findmnt -no SOURCE / )"
root_disk="$(lsblk -no PKNAME "${root_src}" | head -n1)"
if [[ -z "${root_disk}" ]]; then
  # Some setups mount / directly on the whole disk; fall back to basename.
  root_disk="$(basename "${root_src}")"
fi
echo "Root device: ${root_src} (disk: ${root_disk})"

# Pick the first NVMe whole-disk that is not the root disk and has no children.
target=""
while read -r name type; do
  [[ "${type}" == "disk" ]] || continue
  [[ "${name}" == "${root_disk}" ]] && continue
  case "${name}" in
    nvme*)
      # Skip disks that already have partitions/children mounted as root.
      target="${name}"
      break
      ;;
  esac
done < <(lsblk -dno NAME,TYPE)

if [[ -z "${target}" ]]; then
  echo "ERROR: no non-root NVMe instance-store device found." >&2
  lsblk -o NAME,TYPE,SIZE,MOUNTPOINT >&2
  exit 1
fi

dev="/dev/${target}"
echo "Selected instance-store device: ${dev}"

# Guard: only mkfs if there is no existing filesystem on the device.
fstype="$(blkid -o value -s TYPE "${dev}" 2>/dev/null || true)"
if [[ -z "${fstype}" ]]; then
  echo "No filesystem detected on ${dev}; creating ext4..."
  mkfs.ext4 -F "${dev}"
else
  echo "Existing filesystem (${fstype}) on ${dev}; skipping mkfs."
fi

mkdir -p "${MOUNT_POINT}"

if mountpoint -q "${MOUNT_POINT}"; then
  echo "${MOUNT_POINT} already mounted."
else
  echo "Mounting ${dev} at ${MOUNT_POINT}..."
  mount "${dev}" "${MOUNT_POINT}"
fi

mkdir -p "${PG_PARENT}"
# The postgres user is created later by 01_install_pg18.sh, and 10_init_pg.sh
# sets ${PG_PARENT} ownership. Keep /dat root-owned but world-traversable so
# postgres can reach PGDATA underneath it; chown to postgres only once it exists
# (e.g. on a re-run after PG is installed).
chmod 755 "${MOUNT_POINT}"
if id -u postgres >/dev/null 2>&1; then
  chown postgres:postgres "${MOUNT_POINT}" "${PG_PARENT}"
fi

echo "Done."
findmnt "${MOUNT_POINT}"
df -h "${MOUNT_POINT}"
