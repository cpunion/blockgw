# Contributing

Thanks for helping. Changes land through pull requests from forks; nobody pushes to
`dastai/blockgw` directly.

## Workflow

1. Fork `dastai/blockgw` to your account and clone your fork.
2. Create a branch for the change.
3. Make the change, with tests (see below).
4. Push to your fork and open a pull request against `dastai/blockgw:main`.
5. CI must pass, and a maintainer reviews and merges.

## Before opening a pull request

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --release && sudo tests/e2e/run.sh      # needs root and ublk_drv; see docs/testing.md
```

- **Behaviour changes need a test.**
  - Pure logic goes into a unit test next to the code (`src/log.rs`, `src/track.rs`).
  - Anything involving the kernel, crashes, the object store or fencing gets an end-to-end case in
    `tests/e2e/cases/`.
- **A bug fix comes with a regression test** that fails without the fix.
- **Changes to durability, ordering or fencing** must update [docs/design.md](docs/design.md) in the
  same pull request, and must explain in the description why every committed prefix of the log
  still replays to a crash-consistent image.
- **Performance claims** need numbers from `bench/bench.sh` (or a stated method), the host type and
  the object store used.
- **Write all documentation, comments and commit messages in English.**

## Reporting issues

Include the kernel version (`uname -r`), the object store, your configuration (without credentials),
the server log, and the output of `blockgw ctl <socket> stats`.
