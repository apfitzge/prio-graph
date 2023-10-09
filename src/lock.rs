use crate::TransactionId;

/// A read-lock can be held by multiple transactions, and
/// subsequent write-locks should be blocked by all of them.
/// Write-locks are exclusive.
pub(crate) enum Lock<Id: TransactionId> {
    Read(Vec<Id>),
    Write(Id),
}

impl<Id: TransactionId> Lock<Id> {
    /// Take read-lock on a resource.
    /// Returns the id of the write transaction that is blocking the added read.
    pub fn add_read(&mut self, id: Id) -> Option<Id> {
        match self {
            Lock::Read(ids) => {
                ids.push(id);
                None
            }
            Lock::Write(_) => {
                let Lock::Write(id) = core::mem::replace(self, Lock::Read(vec![id])) else {
                    unreachable!("LockKind::Write is guaranteed by match");
                };
                Some(id)
            }
        }
    }

    /// Take write-lock on a resource.
    /// Returns the ids of transactions blocking the added write.
    pub fn add_write(&mut self, id: Id) -> Option<Vec<Id>> {
        match core::mem::replace(self, Lock::Write(id)) {
            Lock::Read(ids) => Some(ids),
            Lock::Write(id) => Some(vec![id]),
        }
    }
}
