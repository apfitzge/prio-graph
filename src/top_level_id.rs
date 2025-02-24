use crate::TransactionId;

/// A top-level ID that should contain the unique `Id`, but can be priority ordered.
pub trait TopLevelId<Id: TransactionId>: Eq + PartialEq + Ord + PartialOrd {
    fn id(&self) -> Id;
}

impl<Id: TransactionId + Ord> TopLevelId<Id> for Id {
    fn id(&self) -> Id {
        *self
    }
}

#[test]
fn integration_test_u64() {
    let id: u64 = 123;
    let toplevel_id = <u64 as TopLevelId<u64>>::id(&id);
    assert_eq!(id, toplevel_id);
}
