use {crate::TransactionId, std::collections::HashSet};

/// A single node in a priority graph that is associated with a `Transaction`.
/// When a node reaches the main queue, the top-level prioritization function can use a reference
/// to this node to calculate a modified priority for the node, which is only used for
/// prioritization for the main queue.
pub struct GraphNode<Id: TransactionId> {
    pub(crate) id: Id,
    /// Whether this node is active, i.e. has not been removed from the main queue.
    pub(crate) active: bool,
    /// Number of edges into this node.
    pub(crate) blocked_by_count: usize,
    /// Unique edges from this node.
    /// The number of edges is the same as the number of forks.
    pub edges: HashSet<usize>,
    /// The distinct chain id of this node.
    pub chain_id: u64,
}
