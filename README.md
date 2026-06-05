# stayputnik - kRPC Client for Rust

[![CI](https://github.com/swgillespie/stayputnik/actions/workflows/ci.yml/badge.svg)](https://github.com/swgillespie/stayputnik/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/stayputnik.svg)](https://crates.io/crates/stayputnik)
[![docs.rs](https://img.shields.io/docsrs/stayputnik)](https://docs.rs/stayputnik)

`stayputnik` is an opinionated, `tokio`-based Rust client for the [kRPC](https://krpc.github.io/krpc/) mod for Kerbal Space Program. Once the mod is installed,
this client will allow you to automate most aspects of Kerbal Space Program; see the official documentation for a full list of what's available.

kRPC is largely self-describing via the `GetServices` call, so this client is mostly auto-generated from a checked-in service response `services.bin`. To support
specific versions of kRPC, regenerate `services.bin` using `cargo run --bin stayputnik-codegen capture <kRPC endpoint>` and then re-run `cargo run --bin stayputnik-codegen`.

This client aims to be complete and faithful to the API surface area that kRPC exposes. It includes a substantial server-side expression library via the `stayputnik::expr` module, though it lacks the sheer expressive power that C# expression trees would have.
