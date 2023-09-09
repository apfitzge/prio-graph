use std::hash::Hash;

/// A unique identifier that can identify resources used by a transaction.
pub trait ResourceKey: Copy + Eq + Hash {}

impl<T: Copy + Eq + Hash> ResourceKey for T {}
