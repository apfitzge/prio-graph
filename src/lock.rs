use {
    crate::{AccessKind, TransactionId},
    smallvec::{smallvec, SmallVec},
};

/// A read-lock can be held by multiple transactions, and
/// subsequent write-locks should be blocked by all of them.
/// Write-locks are exclusive.
pub(crate) struct Lock<Id: TransactionId> {
    kind: AccessKind,
    // Current Reads or Write
    current_locks: SmallVec<[Id; 4]>,
    most_recent_write: Option<Id>,
}

impl<Id: TransactionId> Lock<Id> {
    pub fn new(id: Id, kind: AccessKind) -> Self {
        Self {
            kind,
            current_locks: smallvec![id],
            most_recent_write: None,
        }
    }

    pub fn add_read(&mut self, id: Id, conflict_callback: &mut impl FnMut(&Id)) {
        match self.kind {
            AccessKind::Read => {
                self.current_locks.push(id);
                if let Some(write) = self.most_recent_write.as_ref() {
                    conflict_callback(write);
                }
            }
            AccessKind::Write => {
                debug_assert_eq!(self.current_locks.len(), 1);
                // If the current write is the same as the one we're adding,
                // do not overwrite the write-lock.
                let current_write_id = self.current_locks[0];
                if current_write_id == id {
                    return;
                }

                for id in self.current_locks.drain(..) {
                    conflict_callback(&id);
                }
                self.kind = AccessKind::Read;
                self.current_locks.push(id);
                self.most_recent_write = Some(current_write_id);
            }
        }
    }

    /// Take write-lock on a resource.
    /// For each conflicting lock, call the `conflict_callback`.
    pub fn add_write(&mut self, id: Id, conflict_callback: &mut impl FnMut(&Id)) {
        self.most_recent_write = None;
        match self.kind {
            AccessKind::Read => {
                for id in self.current_locks.drain(..) {
                    conflict_callback(&id);
                }
                self.kind = AccessKind::Write;
                self.current_locks.push(id);
            }
            AccessKind::Write => {
                debug_assert_eq!(self.current_locks.len(), 1);
                let current_write_id = self.current_locks[0];
                if current_write_id == id {
                    return;
                }

                for id in self.current_locks.drain(..) {
                    conflict_callback(&id);
                }
                self.current_locks.push(id);
            }
        }
    }
}
