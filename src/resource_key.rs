use std::hash::Hash;

/// A unique identifier that can identify resources used by a transaction.
pub trait ResourceKey: Copy + Eq + Hash {}
