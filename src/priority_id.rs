use std::{
    fmt::{Debug, Display},
    hash::Hash,
};

/// A unique identifier that can be used both to identify a transaction
/// and to order transactions by priority.
pub trait PriorityId: Copy + Debug + Display + Eq + Ord + Hash {}
