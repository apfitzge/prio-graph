use {crate::PriorityId, std::collections::HashSet};

/// A node in the priority graph.
pub struct GraphNode<Id: PriorityId> {
    pub(crate) active: bool,
    /// Unique edges from this node.
    /// The number of edges is the same as the number of forks.
    pub edges: HashSet<Id>,
    /// Number of edges into this node.
    pub blocked_by_count: usize,
    /// The distinct chain id of this node.
    pub chain_id: u64,
}
