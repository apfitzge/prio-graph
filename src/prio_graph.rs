//! Structure for representing a priority graph - a graph with prioritized
//! edges.
//!
use std::{
    collections::{hash_map::Entry, BinaryHeap, HashMap},
    fmt::Display,
    hash::Hash,
};

/// A unique identifier that can be used both to identify a transaction
/// and to order transactions by priority.
pub trait PriorityId: Copy + Display + Eq + Ord + Hash {}

/// A unique identifier that can identify resources used by a transaction.
pub trait ResourceKey: Copy + Eq + Hash {}

pub trait Transaction<Id: PriorityId, Rk: ResourceKey> {
    fn id(&self) -> Id;

    fn write_locked_resources(&self) -> &[Rk];
    fn read_locked_resources(&self) -> &[Rk];
}

pub struct PrioGraph<Id: PriorityId, Rk: ResourceKey> {
    /// Locked resources and which transaction holds them.
    locks: HashMap<Rk, LockKind<Id>>,
    /// Graph edges and count of edges into each node. The count is used
    /// to detect joins.
    edges: HashMap<Id, EdgesAndCount<Id>>,
    /// Main queue - currently unblocked transactions.
    main_queue: BinaryHeap<Id>,
}

/// A read-lock can be held by multiple transactions, and
/// sub-sequent write-locks should be blocked by all of them.
/// Write-locks are exclusive.
enum LockKind<Id: PriorityId> {
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
            LockKind::Write(id) => Some(vec![id]), // TODO: Remove allocation.
        }
    }
}

#[derive(Default)]
struct EdgesAndCount<Id: PriorityId> {
    /// Edges from this node.
    edges: Vec<Id>,
    /// Number of edges into this node.
    count: usize,
}

impl<'a, Id: PriorityId, Rk: ResourceKey> PrioGraph<Id, Rk> {
    pub fn new<Tx: Transaction<Id, Rk>>(
        transaction_lookup_table: &'a HashMap<Id, Tx>,
        priority_ordered_ids: impl IntoIterator<Item = Id>,
    ) -> Self {
        let mut graph = PrioGraph {
            locks: HashMap::new(),
            edges: HashMap::new(),
            main_queue: BinaryHeap::new(),
        };

        for id in priority_ordered_ids {
            let tx = transaction_lookup_table
                .get(&id)
                .expect("transaction not found");

            let mut incoming_edges_count = 0;
            for read_resource in tx.read_locked_resources() {
                match graph.locks.entry(*read_resource) {
                    Entry::Occupied(mut entry) => {
                        if let Some(blocking_tx) = entry.get_mut().add_read(id) {
                            incoming_edges_count += 1;
                            let edges_and_count = graph
                                .edges
                                .get_mut(&blocking_tx)
                                .expect("blocking_tx must exist");
                            edges_and_count.edges.push(id);
                        }
                    }
                    Entry::Vacant(entry) => {
                        // No existing locks on this resource, simply add.
                        entry.insert(LockKind::Read(vec![id]));
                    }
                }
            }

            for write_resource in tx.write_locked_resources() {
                match graph.locks.entry(*write_resource) {
                    Entry::Occupied(mut entry) => {
                        if let Some(blocking_txs) = entry.get_mut().add_write(id) {
                            incoming_edges_count += blocking_txs.len();
                            for blocking_tx in blocking_txs {
                                let edges_and_count = graph
                                    .edges
                                    .get_mut(&blocking_tx)
                                    .expect("blocking_tx must exist");
                                edges_and_count.edges.push(id);
                            }
                        }
                    }
                    Entry::Vacant(entry) => {
                        // No existing locks on this resource, simply add.
                        entry.insert(LockKind::Write(id));
                    }
                }
            }

            graph.edges.insert(
                id,
                EdgesAndCount {
                    edges: vec![],
                    count: incoming_edges_count,
                },
            );

            // If no incoming edges, this transaction is unblocked.
            if incoming_edges_count == 0 {
                graph.main_queue.push(id);
            }
        }

        graph
    }

    /// Steps the graph forward by one iteration.
    /// Drains all transactions from the primary queue into a batch.
    /// Then, for each transaction in the batch, unblock transactions it was blocking.
    /// If any of those transactions are now unblocked, add them to the primary queue.
    /// Repeat until the primary queue is empty.
    pub fn natural_batches(mut self) -> Vec<Vec<Id>> {
        let mut batches = vec![];

        while !self.main_queue.is_empty() {
            let batch = self.main_queue.drain().collect();

            for id in &batch {
                let edges_and_count = self.edges.remove(id).expect("id must exist");
                for blocked_tx in edges_and_count.edges.iter() {
                    let blocked_tx_edges_and_count = self
                        .edges
                        .get_mut(blocked_tx)
                        .expect("blocked_tx must exist");
                    blocked_tx_edges_and_count.count -= 1;

                    if blocked_tx_edges_and_count.count == 0 {
                        self.main_queue.push(*blocked_tx);
                    }
                }
            }

            batches.push(batch);
        }

        batches
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub type TxId = u64;
    impl PriorityId for TxId {}

    pub type Account = u64;
    impl ResourceKey for Account {}

    pub struct Tx {
        id: TxId,
        read_locked_resources: Vec<Account>,
        write_locked_resources: Vec<Account>,
    }

    impl Transaction<TxId, Account> for Tx {
        fn id(&self) -> TxId {
            self.id
        }

        fn read_locked_resources(&self) -> &[Account] {
            &self.read_locked_resources
        }

        fn write_locked_resources(&self) -> &[Account] {
            &self.write_locked_resources
        }
    }

    // Take in groups of transactions, where each group is a set of transaction ids,
    // and the read and write locked resources for each transaction.
    fn setup_test(
        transaction_groups: impl IntoIterator<Item = (Vec<TxId>, Vec<Account>, Vec<Account>)>,
    ) -> (HashMap<TxId, Tx>, Vec<TxId>) {
        let mut transaction_lookup_table = HashMap::new();
        let mut priority_ordered_ids = vec![];
        for (ids, read_accounts, write_accounts) in transaction_groups {
            for id in &ids {
                priority_ordered_ids.push(*id);
                transaction_lookup_table.insert(
                    *id,
                    Tx {
                        id: *id,
                        read_locked_resources: read_accounts.clone(),
                        write_locked_resources: write_accounts.clone(),
                    },
                );
            }
        }

        priority_ordered_ids.sort_by_key(|id| std::cmp::Reverse(*id));

        (transaction_lookup_table, priority_ordered_ids)
    }

    #[test]
    fn test_simple_queue() {
        // Setup:
        // 3 -> 2 -> 1
        // batches: [3], [2], [1]
        let (transaction_lookup_table, transaction_queue) =
            setup_test([(vec![3, 2, 1], vec![], vec![0])]);
        let graph = PrioGraph::new(&transaction_lookup_table, transaction_queue);
        let batches = graph.natural_batches();
        assert_eq!(batches, [[3], [2], [1]]);
    }

    #[test]
    fn test_multiple_separate_queues() {
        // Setup:
        // 8 -> 4 -> 2 -> 1
        // 7 -> 5 -> 3
        // 6
        // batches: [8, 7, 6], [4, 5], [2, 3], [1]
        let (transaction_lookup_table, transaction_queue) = setup_test([
            (vec![8, 4, 2, 1], vec![], vec![0]),
            (vec![7, 5, 3], vec![], vec![1]),
            (vec![6], vec![], vec![2]),
        ]);

        let graph = PrioGraph::new(&transaction_lookup_table, transaction_queue);
        let batches = graph.natural_batches();
        assert_eq!(batches, [vec![8, 7, 6], vec![5, 4], vec![3, 2], vec![1]]);
    }

    #[test]
    fn test_joining_queues() {
        // Setup:
        // 6 -> 3
        //        \
        //          -> 2 -> 1
        //        /
        // 5 -> 4
        // batches: [6, 5], [3, 4], [2], [1]
        let (transaction_lookup_table, transaction_queue) = setup_test([
            (vec![6, 3], vec![], vec![0]),
            (vec![5, 4], vec![], vec![1]),
            (vec![2, 1], vec![], vec![0, 1]),
        ]);
        let graph = PrioGraph::new(&transaction_lookup_table, transaction_queue);
        let batches = graph.natural_batches();
        assert_eq!(batches, [vec![6, 5], vec![4, 3], vec![2], vec![1]]);
    }

    #[test]
    fn test_forking_queues() {
        // Setup:
        //         -> 2 -> 1
        //        /
        // 6 -> 5
        //        \
        //         -> 4 -> 3
        // batches: [6], [5], [4, 2], [3, 1]
        let (transaction_lookup_table, transaction_queue) = setup_test([
            (vec![6, 5], vec![], vec![0, 1]),
            (vec![2, 1], vec![], vec![0]),
            (vec![4, 3], vec![], vec![1]),
        ]);
        let graph = PrioGraph::new(&transaction_lookup_table, transaction_queue);
        let batches = graph.natural_batches();
        assert_eq!(batches, [vec![6], vec![5], vec![4, 2], vec![3, 1]]);
    }

    #[test]
    fn test_forking_and_joining() {
        // Setup:
        //         -> 5 ----          -> 2 -> 1
        //        /          \      /
        // 9 -> 8              -> 4
        //        \          /      \
        //         -> 7 -> 6          -> 3
        // batches: [9], [8], [7, 5], [6], [4], [3, 2], [1]
        let (transaction_lookup_table, transaction_queue) = setup_test([
            (vec![5, 2, 1], vec![], vec![0]),
            (vec![9, 8, 4], vec![], vec![0, 1]),
            (vec![7, 6, 3], vec![], vec![1]),
        ]);
        let graph = PrioGraph::new(&transaction_lookup_table, transaction_queue);
        let batches = graph.natural_batches();
        assert_eq!(
            batches,
            [
                vec![9],
                vec![8],
                vec![7, 5],
                vec![6],
                vec![4],
                vec![3, 2],
                vec![1]
            ]
        );
    }

    #[test]
    fn test_shared_read_account_no_conflicts() {
        // Setup:
        //   - all transactions read-lock account 0.
        // 8 -> 6 -> 4 -> 2
        // 7 -> 5 -> 3 -> 1
        // Batches: [8, 7], [6, 5], [4, 3], [2, 1]
        let (transaction_lookup_table, transaction_queue) = setup_test([
            (vec![8, 6, 4, 2], vec![0], vec![1]),
            (vec![7, 5, 3, 1], vec![0], vec![2]),
        ]);
        let graph = PrioGraph::new(&transaction_lookup_table, transaction_queue);
        let batches = graph.natural_batches();
        assert_eq!(batches, [vec![8, 7], vec![6, 5], vec![4, 3], vec![2, 1]]);
    }
}
