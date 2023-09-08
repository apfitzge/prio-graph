/// Decision on whether to select a transaction or not while iterating
/// a prio-graph.
pub struct Selection {
    /// Whether or not the transaction was selected.
    pub selected: bool,
    /// Whether or not to continue iterating.
    pub continue_iterating: bool,
}
