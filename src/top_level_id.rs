use {crate::PriorityId, std::cmp::Ordering};

/// An Id that sits at the top of the priority graph.
/// Priority may be modified from the original based on the number of conflicts.
#[derive(Eq, PartialEq)]
pub(crate) struct TopLevelId<Id: PriorityId> {
    pub id: Id,
    pub priority: u64,
}

impl<Id: PriorityId> Ord for TopLevelId<Id> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl<Id: PriorityId> PartialOrd for TopLevelId<Id> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
