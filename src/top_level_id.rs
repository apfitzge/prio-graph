use {crate::TransactionId, std::cmp::Ordering};

/// A top-level ID that should contain the unique `Id`, but can be priority ordered.
pub trait TopLevelId<Id: TransactionId>: Eq + PartialEq + Ord + PartialOrd {
    fn id(&self) -> Id;
}

#[derive(Eq, PartialEq)]
pub(crate) struct TopLevelIdWrapper<Id: TransactionId, Tl: TopLevelId<Id>> {
    pub id: Tl,
    pub index: usize,

    _phantom: core::marker::PhantomData<Id>,
}

impl<Id: TransactionId, Tl: TopLevelId<Id>> TopLevelIdWrapper<Id, Tl> {
    pub fn new(id: Tl, index: usize) -> Self {
        Self {
            id,
            index,
            _phantom: core::marker::PhantomData,
        }
    }
}

impl<Id: TransactionId, Tl: TopLevelId<Id>> Ord for TopLevelIdWrapper<Id, Tl> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl<Id: TransactionId, Tl: TopLevelId<Id>> PartialOrd for TopLevelIdWrapper<Id, Tl> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
