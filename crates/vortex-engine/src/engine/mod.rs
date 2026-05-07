//! Engine-domain layers below RESP command parsing.
//!
//! Modules here coordinate storage, mutation effects, and command-domain
//! behavior without dynamic dispatch.

pub(crate) mod domain;
