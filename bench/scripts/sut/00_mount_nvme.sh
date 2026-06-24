#!/usr/bin/env bash
# Mount non-root NVMe instance store at /dat
set -euo pipefail

MOUNT_POINT=/dat
PG_PARENT="${MOUNT_POINT}/18"

if [[ $EUID -ne 0 ]]; then
  echo "ERROR: must run as root (use sudo)." >&2
  exit 1
fi

# Root filesystem backing disk
root_src="$(findmnt -no SOURCE / )"
root_disk="$(lsblk -no PKNAME "${root_src}" | head -n1)"
if [[ -z "${root_disk}" ]]; then
  # Some setups mount / on whole disk
  root_disk="$(basename "${root_src}")"
fi
echo "Root device: ${root_src} (disk: ${root_disk})"

# Pick first non-root NVMe whole disk
target=""
while read -r name type; do
  [[ "${type}" == "disk" ]] || continue
  [[ "${name}" == "${root_disk}" ]] && continue
  case "${name}" in
    nvme*)
      # Skip root disk
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

# Create filesystem only when absent
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
# Keep /dat traversable until postgres user exists
chmod 755 "${MOUNT_POINT}"
if id -u postgres >/dev/null 2>&1; then
  chown postgres:postgres "${MOUNT_POINT}" "${PG_PARENT}"
fi

echo "Done."
findmnt "${MOUNT_POINT}"
df -h "${MOUNT_POINT}"
