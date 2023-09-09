/// Decision on whether to select a transaction or not while iterating
/// a prio-graph.
pub struct Selection {
    /// Kind of selection.
    pub selected: SelectKind,
    /// Whether or not to continue iterating.
    pub continue_iterating: bool,
}

/// How to handle selected or unselected transactions.
pub enum SelectKind {
    /// The transaction was not selected.
    Unselected,
    /// The transaction was selected, and should not be blocked on.
    SelectedNoBlock,
    /// The transaction was selected, and should be blocked on.
    SelectedBlock,
}
