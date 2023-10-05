//! Structure for representing a priority graph - a graph with prioritized
//! edges.
//!

use {
    crate::{
        lock_kind::LockKind, top_level_id::TopLevelId, AccessKind, GraphNode, PriorityId,
        ResourceKey, Transaction,
    },
    std::{
        cmp::Ordering,
        collections::{hash_map::Entry, BinaryHeap, HashMap, HashSet},
    },
};

/// A directed acyclic graph where edges are only present between nodes if
/// that node is the next-highest priority node for a particular resource.
/// Resources can be either read or write locked with write locks being
/// exclusive.
pub struct PrioGraph<Id: PriorityId, Rk: ResourceKey, Pfn: Fn(&Id, &GraphNode<Id>) -> u64> {
    /// Locked resources and which transaction holds them.
    locks: HashMap<Rk, LockKind<Id>>,
    /// Graph edges and count of edges into each node. The count is used
    /// to detect joins.
    nodes: HashMap<Id, GraphNode<Id>>,
    /// Main queue - currently unblocked transactions.
    main_queue: BinaryHeap<TopLevelId<Id>>,
    /// Priority modification for top-level transactions.
    top_level_prioritization_fn: Pfn,

    /// Used to generate the next distinct chain id.
    next_chain_id: u64,
    /// Map from chain id to joined chain.
    chain_to_joined: HashMap<u64, u64>,
}

impl<Id: PriorityId, Rk: ResourceKey, Pfn: Fn(&Id, &GraphNode<Id>) -> u64> PrioGraph<Id, Rk, Pfn> {
    /// Steps the graph forward by one iteration.
    /// Drains all transactions from the primary queue into a batch.
    /// Then, for each transaction in the batch, unblock transactions it was blocking.
    /// If any of those transactions are now unblocked, add them to the primary queue.
    /// Repeat until the primary queue is empty.
    pub fn natural_batches<'a>(
        iter: impl IntoIterator<Item = (Id, &'a (impl Transaction<Rk> + 'a))>,
        top_level_prioritization_fn: Pfn,
    ) -> Vec<Vec<Id>> {
        // Insert all transactions into the graph.
        let mut graph = PrioGraph::new(top_level_prioritization_fn);
        for (id, tx) in iter.into_iter() {
            graph.insert_transaction(id, tx);
        }

        // Create natural batches by manually popping without unblocking at each level.
        let mut batches = vec![];

        while !graph.main_queue.is_empty() {
            let mut batch = Vec::new();
            while let Some(id) = graph.pop() {
                batch.push(id);
            }

            for id in &batch {
                graph.unblock_id(id);
            }

            batches.push(batch);
        }

        batches
    }

    pub fn new(top_level_prioritization_fn: Pfn) -> Self {
        Self {
            locks: HashMap::new(),
            nodes: HashMap::new(),
            main_queue: BinaryHeap::new(),
            top_level_prioritization_fn,
            next_chain_id: 0,
            chain_to_joined: HashMap::new(),
        }
    }

    pub fn insert_transaction(&mut self, id: Id, tx: &impl Transaction<Rk>) {
        let mut node = GraphNode {
            active: true,
            edges: HashSet::new(),
            blocked_by_count: 0,
            chain_id: self.next_chain_id,
        };

        let mut joined_chains = HashSet::new();

        // Using a macro since a closure cannot be used.
        macro_rules! block_tx {
            ($blocking_id:expr) => {
                let Some(blocking_tx_node) = self.nodes.get_mut(&$blocking_id) else {
                    panic!("blocking node must exist");
                };

                // If the node isn't active then we only do chain tracking.
                if blocking_tx_node.active {
                    // Add edges to the current node.
                    // If it is a unique edge, increment the blocked_by_count for the current node.
                    if blocking_tx_node.edges.insert(id) {
                        node.blocked_by_count += 1;
                    }
                }
                let blocking_chain_id = blocking_tx_node.chain_id;
                let blocking_chain_id = self.trace_chain(blocking_chain_id);

                match node.chain_id.cmp(&blocking_chain_id) {
                    Ordering::Less => {
                        joined_chains.insert(blocking_chain_id);
                    }
                    Ordering::Equal => {}
                    Ordering::Greater => {
                        joined_chains.insert(node.chain_id);
                        node.chain_id = blocking_chain_id;
                    }
                }
            };
        }

        tx.check_resource_keys(&mut |resource_key: &Rk, access_kind: AccessKind| match self
            .locks
            .entry(*resource_key)
        {
            Entry::Vacant(entry) => {
                entry.insert(match access_kind {
                    AccessKind::Read => LockKind::Read(vec![id]),
                    AccessKind::Write => LockKind::Write(id),
                });
            }
            Entry::Occupied(mut entry) => match access_kind {
                AccessKind::Read => {
                    if let Some(blocking_tx) = entry.get_mut().add_read(id) {
                        block_tx!(blocking_tx);
                    }
                }
                AccessKind::Write => {
                    if let Some(blocking_txs) = entry.get_mut().add_write(id) {
                        for blocking_tx in blocking_txs {
                            block_tx!(blocking_tx);
                        }
                    }
                }
            },
        });

        // If this chain id is distinct, then increment the `next_chain_id`.
        if node.chain_id == self.next_chain_id {
            self.next_chain_id += 1;
        }

        // Add all joining chains into the map.
        for joining_chain in joined_chains {
            self.chain_to_joined.insert(joining_chain, node.chain_id);
        }

        self.nodes.insert(id, node);

        // If the node is not blocked, add it to the main queue.
        if self.nodes.get(&id).unwrap().blocked_by_count == 0 {
            self.main_queue.push(self.create_top_level_id(id));
        }
    }

    /// Returns true if the top-of-queue is empty.
    pub fn is_empty(&self) -> bool {
        self.main_queue.is_empty()
    }

    /// Returns the minimum chain id for a given node id.
    ///
    /// Panics:
    ///     - Node does not exist.
    pub fn chain_id(&self, id: &Id) -> u64 {
        self.trace_chain(self.nodes.get(id).unwrap().chain_id)
    }

    /// Combination of `pop` and `unblock_id`.
    pub fn pop_and_unblock(&mut self) -> Option<Id> {
        let id = self.pop()?;
        self.unblock_id(&id);
        Some(id)
    }

    /// Pop the highest priority node id from the queue.
    /// Returns None if the queue is empty.
    pub fn pop(&mut self) -> Option<Id> {
        self.main_queue.pop().map(|top_level_id| top_level_id.id)
    }

    /// This will unblock transactions that were blocked by this transaction.
    ///
    /// Panics:
    ///     - Node does not exist.
    ///     - If the node.blocked_by_count != 0
    pub fn unblock_id(&mut self, id: &Id) {
        // If the node is already removed, do nothing.
        let Some(node) = self.nodes.get_mut(id) else {
            panic!("node must exist");
        };
        assert_eq!(node.blocked_by_count, 0, "node must be unblocked");

        node.active = false;
        let edges = core::mem::take(&mut node.edges);

        // Unblock transactions that were blocked by this node.
        for blocked_tx in edges.iter() {
            let blocked_tx_node = self
                .nodes
                .get_mut(blocked_tx)
                .expect("blocked_tx must exist");
            blocked_tx_node.blocked_by_count -= 1;

            if blocked_tx_node.blocked_by_count == 0 {
                self.main_queue.push(self.create_top_level_id(*blocked_tx));
            }
        }
    }

    fn create_top_level_id(&self, id: Id) -> TopLevelId<Id> {
        TopLevelId {
            id,
            priority: (self.top_level_prioritization_fn)(&id, self.nodes.get(&id).unwrap()),
        }
    }

    fn trace_chain(&self, mut chain_id: u64) -> u64 {
        while let Some(joined_chain) = self.chain_to_joined.get(&chain_id) {
            chain_id = *joined_chain;
        }
        chain_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub type TxId = u64;

    pub type Account = u64;

    pub struct Tx {
        read_locked_resources: Vec<Account>,
        write_locked_resources: Vec<Account>,
    }

    impl Transaction<Account> for Tx {
        fn check_resource_keys<F: FnMut(&Account, AccessKind)>(&self, mut checker: F) {
            for account in &self.read_locked_resources {
                checker(account, AccessKind::Read);
            }
            for account in &self.write_locked_resources {
                checker(account, AccessKind::Write);
            }
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
                        read_locked_resources: read_accounts.clone(),
                        write_locked_resources: write_accounts.clone(),
                    },
                );
            }
        }

        // Sort in reverse priority order - highest priority first.
        priority_ordered_ids.sort_by(|a, b| b.cmp(a));

        (transaction_lookup_table, priority_ordered_ids)
    }

    fn create_lookup_iterator<'a>(
        transaction_lookup_table: &'a HashMap<TxId, Tx>,
        reverse_priority_order_ids: &'a [TxId],
    ) -> impl Iterator<Item = (TxId, &'a Tx)> + 'a {
        reverse_priority_order_ids.iter().map(|id| {
            (
                *id,
                transaction_lookup_table.get(id).expect("id must exist"),
            )
        })
    }

    fn test_top_level_priority_fn(id: &TxId, _node: &GraphNode<TxId>) -> u64 {
        *id
    }

    #[test]
    fn test_simple_queue() {
        // Setup:
        // 3 -> 2 -> 1
        // batches: [3], [2], [1]
        let (transaction_lookup_table, transaction_queue) =
            setup_test([(vec![3, 2, 1], vec![], vec![0])]);
        let batches = PrioGraph::natural_batches(
            create_lookup_iterator(&transaction_lookup_table, &transaction_queue),
            test_top_level_priority_fn,
        );
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
        let batches = PrioGraph::natural_batches(
            create_lookup_iterator(&transaction_lookup_table, &transaction_queue),
            test_top_level_priority_fn,
        );
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
        let batches = PrioGraph::natural_batches(
            create_lookup_iterator(&transaction_lookup_table, &transaction_queue),
            test_top_level_priority_fn,
        );
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
        let batches = PrioGraph::natural_batches(
            create_lookup_iterator(&transaction_lookup_table, &transaction_queue),
            test_top_level_priority_fn,
        );
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
        let batches = PrioGraph::natural_batches(
            create_lookup_iterator(&transaction_lookup_table, &transaction_queue),
            test_top_level_priority_fn,
        );
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
        let batches = PrioGraph::natural_batches(
            create_lookup_iterator(&transaction_lookup_table, &transaction_queue),
            test_top_level_priority_fn,
        );
        assert_eq!(batches, [vec![8, 7], vec![6, 5], vec![4, 3], vec![2, 1]]);
    }
}
