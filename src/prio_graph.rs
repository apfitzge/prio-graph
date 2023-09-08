//! Structure for representing a priority graph - a graph with prioritized
//! edges.
//!
use std::{
    collections::{BinaryHeap, HashMap},
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

    // simplify for now and assume all are write-locked
    fn locked_resources(&self) -> &[Rk];
    // fn write_locked_resources(&self) -> &[Rk];
    // fn read_locked_resources(&self) -> &[Rk];
}

pub struct PrioGraph<Id: PriorityId, Rk: ResourceKey> {
    /// Locked resources and which transaction holds them.
    locks: HashMap<Rk, Id>,
    /// Graph edges and count of edges into each node. The count is used
    /// to detect joins.
    edges: HashMap<Id, EdgesAndCount<Id>>,
    /// Main queue - currently unblocked transactions.
    main_queue: BinaryHeap<Id>,
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

            let resources = tx.locked_resources();
            let mut incoming_edges_count = 0;
            for resource in resources {
                if let Some(other_id) = graph.locks.get(resource) {
                    incoming_edges_count += 1;
                    let edges_and_count = graph.edges.get_mut(other_id).expect("id must exist");
                    edges_and_count.edges.push(id);
                }

                // we always take the locks - even if they are already taken.
                // this makes it so subsequent transactions will be blocked by
                // this transaction, rather than the higher priority one they
                // are both blocked by.
                graph.locks.insert(*resource, id);
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
    pub fn natural_batches(&mut self) -> Vec<Vec<Id>> {
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
        locked_resources: Vec<Account>,
    }

    impl Transaction<TxId, Account> for Tx {
        fn id(&self) -> TxId {
            self.id
        }

        fn locked_resources(&self) -> &[Account] {
            &self.locked_resources
        }
    }

    // Take in groups of transactions, where each group is a set of transaction ids,
    // and the set of acccounts they lock.
    fn setup_test(
        transaction_groups: impl IntoIterator<Item = (Vec<TxId>, Vec<Account>)>,
    ) -> (HashMap<TxId, Tx>, Vec<TxId>) {
        let mut transaction_lookup_table = HashMap::new();
        let mut priority_ordered_ids = vec![];
        for (ids, accounts) in transaction_groups {
            for id in &ids {
                priority_ordered_ids.push(*id);
                transaction_lookup_table.insert(
                    *id,
                    Tx {
                        id: *id,
                        locked_resources: accounts.clone(),
                    },
                );
            }
        }

        priority_ordered_ids.sort_by_key(|id| std::cmp::Reverse(*id));

        (transaction_lookup_table, priority_ordered_ids)
    }

    #[test]
    fn simple_queue() {
        // Setup:
        // 3 -> 2 -> 1
        // batches: [3], [2], [1]
        let (transaction_lookup_table, transaction_queue) = setup_test([(vec![3, 2, 1], vec![0])]);
        let mut graph = PrioGraph::new(&transaction_lookup_table, transaction_queue);
        let batches = graph.natural_batches();
        assert_eq!(batches, [[3], [2], [1]]);
    }

    #[test]
    fn multiple_separate_queues() {
        // Setup:
        // 8 -> 4 -> 2 -> 1
        // 7 -> 5 -> 3
        // 6
        // batches: [8, 7, 6], [4, 5], [2, 3], [1]
        let (transaction_lookup_table, transaction_queue) = setup_test([
            (vec![8, 4, 2, 1], vec![0]),
            (vec![7, 5, 3], vec![1]),
            (vec![6], vec![2]),
        ]);

        let mut graph = PrioGraph::new(&transaction_lookup_table, transaction_queue);
        let batches = graph.natural_batches();
        assert_eq!(batches, [vec![8, 7, 6], vec![5, 4], vec![3, 2], vec![1]]);
    }
}
