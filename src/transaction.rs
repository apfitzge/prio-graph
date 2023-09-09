use crate::{PriorityId, ResourceKey};

pub enum AccessKind {
    Read,
    Write,
}

pub trait Transaction<Id: PriorityId, Rk: ResourceKey> {
    fn id(&self) -> Id;
    fn reward(&self) -> u64;
    fn check_resource_keys<F: FnMut(&Rk, AccessKind)>(&self, checker: F);
}
