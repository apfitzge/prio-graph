use crate::PriorityId;

/// A top-level ID that should contain the unique `Id`, but can be priority ordered differently.
pub trait TopLevelId<Id: PriorityId>: Eq + PartialEq + Ord + PartialOrd {
    fn id(&self) -> Id;
}
