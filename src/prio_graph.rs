//! Structure for representing a priority graph - a graph with prioritized
//! edges.
//!
use core::hash::Hash;
use std::collections::{BinaryHeap, HashMap};

/// A unique identifier that can be used both to identify a transaction
/// and to order transactions by priority.
pub trait PriorityId: Copy + Eq + Ord + Hash {}

/// A unique identifier that can identify resources used by a transaction.
pub trait ResourceKey: Copy + Eq + Hash {}

pub trait Transaction<Id: PriorityId, Rk: ResourceKey> {
    fn id(&self) -> Id;

    // simplify for now and assume all are write-locked
    fn locked_resources(&self) -> &[Rk];
    // fn write_locked_resources(&self) -> &[Rk];
    // fn read_locked_resources(&self) -> &[Rk];
}

pub struct PrioGraph<'a, Id: PriorityId, Rk: ResourceKey, Tx: Transaction<Id, Rk>> {
    /// Transaction lookup table
    transactions: &'a HashMap<Id, Tx>,
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

impl<'a, Id: PriorityId, Rk: ResourceKey, Tx: Transaction<Id, Rk>> PrioGraph<'a, Id, Rk, Tx> {
    pub fn new(
        transaction_lookup_table: &'a HashMap<Id, Tx>,
        priority_ordered_ids: impl Iterator<Item = Id>,
    ) -> Self {
        let mut graph = PrioGraph {
            transactions: transaction_lookup_table,
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
                let Some(other_id) = graph.locks.get(resource) else {
                    continue;
                };

                incoming_edges_count += 1;
                let edges_and_count = graph.edges.get_mut(other_id).expect("id must exist");
                edges_and_count.edges.push(id);
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
