use crate::{transaction_key::TransactionKey, TransactionId};

/// A top-level ID that should contain the unique `Id`, but can be priority ordered.
pub trait TopLevelId<Id: TransactionId>: Eq + PartialEq + Ord + PartialOrd {
    fn id(&self) -> Id;
}

pub(crate) struct TopLevelIdWrapper<Id: TransactionId, Tl: TopLevelId<Id>> {
    pub top_level_id: Tl,
    pub index: usize,

    _id: core::marker::PhantomData<Id>,
}

impl<Id: TransactionId, Tl: TopLevelId<Id>> TopLevelIdWrapper<Id, Tl> {
    pub(crate) fn new(top_level_id: Tl, index: usize) -> Self {
        Self {
            top_level_id,
            index,
            _id: core::marker::PhantomData,
        }
    }
}

impl<Id: TransactionId, Tl: TopLevelId<Id>> Eq for TopLevelIdWrapper<Id, Tl> {}

impl<Id: TransactionId, Tl: TopLevelId<Id>> PartialEq for TopLevelIdWrapper<Id, Tl> {
    fn eq(&self, other: &Self) -> bool {
        self.top_level_id == other.top_level_id
    }
}

impl<Id: TransactionId, Tl: TopLevelId<Id>> Ord for TopLevelIdWrapper<Id, Tl> {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        self.top_level_id.cmp(&other.top_level_id)
    }
}

impl<Id: TransactionId, Tl: TopLevelId<Id>> PartialOrd for TopLevelIdWrapper<Id, Tl> {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<Id: TransactionId, Tl: TopLevelId<Id>> From<TopLevelIdWrapper<Id, Tl>> for TransactionKey<Id> {
    fn from(wrapper: TopLevelIdWrapper<Id, Tl>) -> Self {
        TransactionKey::new(wrapper.top_level_id.id(), wrapper.index)
    }
}
