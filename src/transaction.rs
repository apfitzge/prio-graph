use crate::ResourceKey;

/// Type of access for a specific resource key.
pub enum AccessKind {
    Read,
    Write,
}

/// A transaction will access a set of resource keys with various `AccessKind`s.
pub trait Transaction<Rk: ResourceKey> {
    // The interface is a bit awkward, as rust does not currently support something like:
    // ```rust
    // fn check_resource_keys(&self) -> impl Iterator<Item = (&Rk, AccessKind)>;
    // ```
    // See: https://github.com/rust-lang/rust/issues/91611
    /// Used by the `PrioGraph` to check and track conflicts in resource usage.
    fn check_resource_keys<F: FnMut(&Rk, AccessKind)>(&self, checker: F);
}
