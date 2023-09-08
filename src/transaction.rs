use crate::{PriorityId, ResourceKey};

pub trait Transaction<Id: PriorityId, Rk: ResourceKey> {
    fn id(&self) -> Id;

    fn write_locked_resources(&self) -> &[Rk];
    fn read_locked_resources(&self) -> &[Rk];
}
