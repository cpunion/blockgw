# Local development

blockgw needs a Linux kernel with `ublk_drv` and root. The object store is optional: a local directory
(`"local_store"`) works as the segment log, so a laptop needs no S3 at all.

## Linux

```sh
sudo apt-get install -y clang libclang-dev xfsprogs   # Debian/Ubuntu; Ubuntu also needs linux-modules-extra-$(uname -r)
sudo modprobe ublk_drv
cargo build --release
sudo target/release/blockgw serve examples/config.local.json &
sudo mkfs.xfs /dev/ublkb0 && sudo mkdir -p /mnt/disk0 && sudo mount /dev/ublkb0 /mnt/disk0
```

## macOS and Windows

Neither has ublk, so blockgw runs inside a Linux VM. The filesystem on the disk is shared back to the
host as a directory.

| Host | VM | ublk | Sharing the mounted disk back to the host |
|---|---|---|---|
| macOS | [OrbStack](https://orbstack.dev) (machine or privileged container) | built into OrbStack's kernel (verified, 7.0) | OrbStack exposes machine files under `~/OrbStack/<machine>/`, or export via NFS/SMB |
| macOS | Lima / Colima with a Debian 13 image | `modprobe ublk_drv` (Debian 13 kernel) | Lima mounts, NFS or SMB |
| Windows | WSL2 | not verified: the stock WSL2 kernel may not enable `CONFIG_BLK_DEV_UBLK`; a custom WSL kernel can | `\\wsl$\<distro>\...` |
| Windows | Hyper-V or another hypervisor with Debian 13 | `modprobe ublk_drv` | SMB |

The end-to-end suite runs the same way on all of them: build a Linux binary, for example in a `rust`
container, then run `tests/e2e/run.sh` in a privileged container or VM. See
[testing.md](testing.md).

```sh
# OrbStack or Docker Desktop on macOS, local-directory store (no S3 needed)
docker run --rm -v "$PWD":/src -w /src rust:1-bookworm bash -c \
  'apt-get update -qq && apt-get install -y -qq clang libclang-dev >/dev/null && cargo build --release'
docker run --rm --privileged -v /dev:/dev -v "$PWD":/repo debian:trixie-slim bash -c \
  'apt-get update -qq && apt-get install -y -qq xfsprogs curl python3 procps util-linux bc kmod >/dev/null &&
   E2E_STORE=dir BLOCKGW=/repo/target/release/blockgw /repo/tests/e2e/run.sh'
```

## Native macOS and Windows block devices (not implemented)

These are possible future frontends; each needs its own work:

- **Windows**:
  - The built-in iSCSI initiator would mount a blockgw iSCSI target natively, with no driver
    needed on the client.
  - Alternatively, a user-mode storage proxy driver such as [WinSpd](https://github.com/winfsp/winspd)
    could serve the disk.
- **macOS**:
  - Serve the disk as a raw image file through a userspace filesystem (FSKit on macOS 15.4+, or
    macFUSE), then attach it with `hdiutil attach`. This goes through an extra file layer, so its
    performance is untested.
  - A DriverKit block storage driver requires entitlements from Apple.

Until then, a Linux VM is the supported way to run blockgw on these hosts.
