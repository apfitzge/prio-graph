use std::hash::Hash;

/// A unique identifier that can be used both to identify a transaction
/// and to order transactions by priority.
pub trait PriorityId: Copy + Eq + Hash {}

impl<T: Copy + Eq + Hash> PriorityId for T {}
