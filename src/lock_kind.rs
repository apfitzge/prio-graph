use crate::PriorityId;

/// A read-lock can be held by multiple transactions, and
/// subsequent write-locks should be blocked by all of them.
/// Write-locks are exclusive.
pub(crate) enum LockKind<Id: PriorityId> {
    Read(Vec<Id>),
    Write(Id),
}

impl<Id: PriorityId> LockKind<Id> {
    /// Take read-lock on a resource.
    /// Returns the id of the write transaction that is blocking the added read.
    pub fn add_read(&mut self, id: Id) -> Option<Id> {
        match self {
            LockKind::Read(ids) => {
                ids.push(id);
                None
            }
            LockKind::Write(_) => {
                let LockKind::Write(id) = core::mem::replace(self, LockKind::Read(vec![id])) else {
                    unreachable!("LockKind::Write is guaranteed by match");
                };
                Some(id)
            }
        }
    }

    /// Take write-lock on a resource.
    /// Returns the ids of transactions blocking the added write.
    pub fn add_write(&mut self, id: Id) -> Option<Vec<Id>> {
        match core::mem::replace(self, LockKind::Write(id)) {
            LockKind::Read(ids) => Some(ids),
            LockKind::Write(id) => Some(vec![id]),
        }
    }
}
