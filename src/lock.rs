/// A read-lock can be held by multiple transactions, and
/// subsequent write-locks should be blocked by all of them.
/// Write-locks are exclusive.
pub(crate) enum Lock {
    Read(Vec<usize>, Option<usize>), // (Current Reads, Most Recent Write)
    Write(usize),
}

impl Lock {
    /// Take read-lock on a resource.
    /// Returns the id of the write transaction that is blocking the added read.
    pub fn add_read(&mut self, index: usize) -> Option<usize> {
        match self {
            Lock::Read(indices, maybe_write) => {
                indices.push(index);
                *maybe_write
            }
            Lock::Write(current_write_index) => {
                // If the current write is the same as the one we're adding,
                // do not overwrite the write-lock.
                let current_write_index = *current_write_index;
                if current_write_index == index {
                    return None;
                }
                let Lock::Write(index) =
                    core::mem::replace(self, Lock::Read(vec![index], Some(current_write_index)))
                else {
                    unreachable!("LockKind::Write is guaranteed by match");
                };
                Some(index)
            }
        }
    }

    /// Take write-lock on a resource.
    /// Returns the ids of transactions blocking the added write.
    pub fn add_write(&mut self, index: usize) -> Option<Vec<usize>> {
        match core::mem::replace(self, Lock::Write(index)) {
            Lock::Read(indices, _) => Some(indices),
            Lock::Write(current_write_index) => {
                if current_write_index == index {
                    None
                } else {
                    Some(vec![current_write_index])
                }
            }
        }
    }
}
