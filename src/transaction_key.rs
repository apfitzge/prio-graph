use crate::TransactionId;

/// TransactionKey is a struct that can be used to uniquely identify a transaction.
/// It wraps a `TransactionId`, and the `id` can be extracted from it.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TransactionKey<Id: TransactionId> {
    /// Wrapped `TransactionId`
    id: Id,
    /// Index into internal structures for the transaction's
    /// node in the graph. This is not needed by code outside
    /// of this crate, and is only used internally.
    index: usize,
}

impl<Id: TransactionId> TransactionKey<Id> {
    /// Creates a new `TransactionKey` from a `TransactionId` and an index.
    /// This function is only used internally, and should not be used by
    /// code outside of this crate.
    pub(crate) fn new(id: Id, index: usize) -> Self {
        Self { id, index }
    }

    /// Returns the `TransactionId` wrapped by this `TransactionKey`.
    pub fn id(&self) -> &Id {
        &self.id
    }

    /// Returns the index of the transaction's node in the graph.
    /// This is not needed by code outside of this crate, and is only
    /// used internally.
    pub(crate) fn index(&self) -> usize {
        self.index
    }
}
