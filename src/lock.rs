/// A read-lock can be held by multiple transactions, and
/// subsequent write-locks should be blocked by all of them.
/// Write-locks are exclusive.
pub(crate) enum Lock {
    Read(Vec<usize>),
    Write(usize),
}

impl Lock {
    /// Take read-lock on a resource.
    /// Returns the index of the write transaction that is blocking the added read.
    pub fn add_read(&mut self, index: usize) -> Option<usize> {
        match self {
            Lock::Read(indexes) => {
                indexes.push(index);
                None
            }
            Lock::Write(_) => {
                let Lock::Write(index) = core::mem::replace(self, Lock::Read(vec![index])) else {
                    unreachable!("LockKind::Write is guaranteed by match");
                };
                Some(index)
            }
        }
    }

    /// Take write-lock on a resource.
    /// Returns the index of transactions blocking the added write.
    pub fn add_write(&mut self, index: usize) -> Option<Vec<usize>> {
        match core::mem::replace(self, Lock::Write(index)) {
            Lock::Read(indexes) => Some(indexes),
            Lock::Write(index) => Some(vec![index]),
        }
    }
}
