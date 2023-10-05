use crate::ResourceKey;

pub enum AccessKind {
    Read,
    Write,
}

pub trait Transaction<Rk: ResourceKey> {
    fn check_resource_keys<F: FnMut(&Rk, AccessKind)>(&self, checker: F);
}
