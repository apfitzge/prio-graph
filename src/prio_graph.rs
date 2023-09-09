//! Structure for representing a priority graph - a graph with prioritized
//! edges.
//!

use {
    crate::{selection::Selection, AccessKind, PriorityId, ResourceKey, SelectKind, Transaction},
    std::{
        cmp::Ordering,
        collections::{hash_map::Entry, BinaryHeap, HashMap, HashSet},
    },
};

/// A directed acyclic graph where edges are only present between nodes if
/// that node is the next-highest priority node for a particular resource.
/// Resources can be either read or write locked with write locks being
/// exclusive.
pub struct PrioGraph<Id: PriorityId, Rk: ResourceKey, Pfn: Fn(Id, u64) -> u64> {
    /// Locked resources and which transaction holds them.
    locks: HashMap<Rk, LockKind<Id>>,
    /// Graph edges and count of edges into each node. The count is used
    /// to detect joins.
    nodes: HashMap<Id, GraphNode<Id>>,
    /// Main queue - currently unblocked transactions.
    main_queue: BinaryHeap<TopLevelId<Id>>,
    /// Priority modification for top-level transactions.
    top_level_prioritization_fn: Pfn,
}

/// An Id that sits at the top of the priority graph.
/// Priority may be modified from the original based on the number of conflicts.
#[derive(Eq, PartialEq)]
struct TopLevelId<Id: PriorityId> {
    id: Id,
    priority: u64,
}

impl<Id: PriorityId> Ord for TopLevelId<Id> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl<Id: PriorityId> PartialOrd for TopLevelId<Id> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
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

/// A node in the priority graph.
pub struct GraphNode<Id: PriorityId> {
    /// Unique edges from this node.
    /// The number of edges is the same as the number of forks.
    pub edges: HashSet<Id>,
    /// Reward at next level - i.e. how much is blocked by this.
    pub next_level_rewards: u64,
    /// Number of edges into this node.
    pub blocked_by_count: usize,
}

impl<'a, Id: PriorityId, Rk: ResourceKey, Pfn: Fn(Id, u64) -> u64> PrioGraph<Id, Rk, Pfn> {
    pub fn new<Tx: Transaction<Id, Rk>>(
        transaction_lookup_table: &'a HashMap<Id, Tx>,
        reverse_priority_ordered_ids: impl IntoIterator<Item = Id>,
        top_level_prioritization_fn: Pfn,
    ) -> Self {
        let mut graph = PrioGraph {
            locks: HashMap::new(),
            nodes: HashMap::new(),
            main_queue: BinaryHeap::new(),
            top_level_prioritization_fn,
        };

        let mut currently_unblocked = HashSet::new();

        for id in reverse_priority_ordered_ids {
            let tx = transaction_lookup_table
                .get(&id)
                .expect("transaction not found");
            // TODO: Resizing edges is expensive. We might be better off with an adjacency list.
            let mut node = GraphNode {
                edges: HashSet::new(),
                next_level_rewards: 0,
                blocked_by_count: 0,
            };

            // Add id into currently unblocked set. It will be removed later if something blocks it.
            currently_unblocked.insert(id);

            let mut block_tx = |blocked_tx: Id| {
                let blocked_tx_node = graph
                    .nodes
                    .get_mut(&blocked_tx)
                    .expect("blocked_tx must exist");
                // If this transaction was previously unblocked, remove it from the
                // unblocked set.
                currently_unblocked.remove(&blocked_tx);

                // Add edges out of current node, update the total blocked count.
                if node.edges.insert(blocked_tx) {
                    node.next_level_rewards += transaction_lookup_table
                        .get(&blocked_tx)
                        .expect("blocked_tx must exist")
                        .reward();
                    blocked_tx_node.blocked_by_count += 1;
                }
            };

            tx.check_resource_keys(
                &mut |resource_key: &Rk, access_kind: AccessKind| match graph
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
                            if let Some(blocked_tx) = entry.get_mut().add_read(id) {
                                block_tx(blocked_tx);
                            }
                        }
                        AccessKind::Write => {
                            if let Some(blocked_txs) = entry.get_mut().add_write(id) {
                                for blocked_tx in blocked_txs {
                                    block_tx(blocked_tx);
                                }
                            }
                        }
                    },
                },
            );
            graph.nodes.insert(id, node);
        }

        let top_level_ids: Vec<_> = currently_unblocked
            .into_iter()
            .map(|id| graph.create_top_level_id(id))
            .collect();
        graph.main_queue.extend(top_level_ids);
        graph
    }

    /// Returns true if the top-of-queue is empty.
    pub fn is_empty(&self) -> bool {
        self.main_queue.is_empty()
    }

    /// Callback controlled iteration through current top-level of the graph.
    pub fn iterate<Selector: FnMut(Id, &GraphNode<Id>) -> Selection>(
        &mut self,
        mut selector: Selector,
    ) {
        let mut add_back = vec![];
        while let Some(top_level_id) = self.main_queue.pop() {
            let node = self.nodes.get(&top_level_id.id).expect("id must exist");

            let Selection {
                selected,
                continue_iterating,
            } = selector(top_level_id.id, node);

            // Node was selected, we must unblock the transactions it was blocking.
            match selected {
                SelectKind::Unselected => add_back.push(top_level_id),
                SelectKind::SelectedNoBlock => self.remove_transaction(&top_level_id.id),
                SelectKind::SelectedBlock => {}
            }

            if !continue_iterating {
                break;
            }
        }

        self.main_queue.extend(add_back);
    }

    /// Steps the graph forward by one iteration.
    /// Drains all transactions from the primary queue into a batch.
    /// Then, for each transaction in the batch, unblock transactions it was blocking.
    /// If any of those transactions are now unblocked, add them to the primary queue.
    /// Repeat until the primary queue is empty.
    pub fn natural_batches(mut self) -> Vec<Vec<Id>> {
        let mut batches = vec![];

        while !self.main_queue.is_empty() {
            let mut batch = Vec::with_capacity(self.main_queue.len());
            while let Some(top_level_id) = self.main_queue.pop() {
                batch.push(top_level_id.id);
            }

            for id in &batch {
                self.remove_transaction(id);
            }

            batches.push(batch);
        }

        batches
    }

    /// Remove a top-level transaction.
    /// This will unblock transactions that were blocked by this transaction.
    ///
    /// Panics:
    ///     - If the node.blocked_by_count != 0
    pub fn remove_transaction(&mut self, id: &Id) {
        // If the node is already removed, do nothing.
        let Some(node) = self.nodes.remove(id) else {
            return;
        };
        assert_eq!(node.blocked_by_count, 0, "node must be unblocked");

        // Unblock transactions that were blocked by this node.
        for blocked_tx in node.edges.iter() {
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
            priority: (self.top_level_prioritization_fn)(
                id,
                self.nodes.get(&id).unwrap().next_level_rewards,
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, std::cell::RefCell};

    pub type TxId = u64;

    pub type Account = u64;

    pub struct Tx {
        id: TxId,
        read_locked_resources: Vec<Account>,
        write_locked_resources: Vec<Account>,
    }

    impl Transaction<TxId, Account> for Tx {
        fn id(&self) -> TxId {
            self.id
        }

        fn reward(&self) -> u64 {
            1
        }

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
                        id: *id,
                        read_locked_resources: read_accounts.clone(),
                        write_locked_resources: write_accounts.clone(),
                    },
                );
            }
        }

        // Sort in reverse priority order - lowest priority first.
        priority_ordered_ids.sort();

        (transaction_lookup_table, priority_ordered_ids)
    }

    fn test_top_level_priority_fn(id: TxId, _next_level_rewards: u64) -> u64 {
        id
    }

    #[test]
    fn test_simple_queue() {
        // Setup:
        // 3 -> 2 -> 1
        // batches: [3], [2], [1]
        let (transaction_lookup_table, transaction_queue) =
            setup_test([(vec![3, 2, 1], vec![], vec![0])]);
        let graph = PrioGraph::new(
            &transaction_lookup_table,
            transaction_queue,
            test_top_level_priority_fn,
        );
        assert!(!graph.is_empty());
        for (expected_next_level_reward, ids) in [(1, vec![3, 2]), (0, vec![1])] {
            for id in ids {
                assert_eq!(
                    graph.nodes.get(&id).unwrap().next_level_rewards,
                    expected_next_level_reward
                );
            }
        }

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

        let graph = PrioGraph::new(
            &transaction_lookup_table,
            transaction_queue,
            test_top_level_priority_fn,
        );
        assert!(!graph.is_empty());
        for (expected_next_level_reward, ids) in [(1, vec![8, 7, 5, 4, 2]), (0, vec![6, 3, 1])] {
            for id in ids {
                assert_eq!(
                    graph.nodes.get(&id).unwrap().next_level_rewards,
                    expected_next_level_reward
                );
            }
        }

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
        let graph = PrioGraph::new(
            &transaction_lookup_table,
            transaction_queue,
            test_top_level_priority_fn,
        );
        assert!(!graph.is_empty());
        for (expected_next_level_reward, ids) in [(1, vec![6, 5, 4, 3, 2]), (0, vec![1])] {
            for id in ids {
                assert_eq!(
                    graph.nodes.get(&id).unwrap().next_level_rewards,
                    expected_next_level_reward
                );
            }
        }

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
        let graph = PrioGraph::new(
            &transaction_lookup_table,
            transaction_queue,
            test_top_level_priority_fn,
        );
        assert!(!graph.is_empty());
        for (expected_next_level_reward, ids) in [(2, vec![5]), (1, vec![6, 4, 2]), (0, vec![3, 1])]
        {
            for id in ids {
                assert_eq!(
                    graph.nodes.get(&id).unwrap().next_level_rewards,
                    expected_next_level_reward
                );
            }
        }

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
        let graph = PrioGraph::new(
            &transaction_lookup_table,
            transaction_queue,
            test_top_level_priority_fn,
        );
        assert!(!graph.is_empty());
        for (expected_next_level_reward, ids) in
            [(2, vec![8, 4]), (1, vec![9, 7, 6, 5, 2]), (0, vec![3, 1])]
        {
            for id in ids {
                assert_eq!(
                    graph.nodes.get(&id).unwrap().next_level_rewards,
                    expected_next_level_reward
                );
            }
        }

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
        let graph = PrioGraph::new(
            &transaction_lookup_table,
            transaction_queue,
            test_top_level_priority_fn,
        );
        assert!(!graph.is_empty());
        for (expected_next_level_reward, ids) in [(1, vec![8, 7, 6, 5, 4, 3]), (0, vec![2, 1])] {
            for id in ids {
                assert_eq!(
                    graph.nodes.get(&id).unwrap().next_level_rewards,
                    expected_next_level_reward
                );
            }
        }

        let batches = graph.natural_batches();
        assert_eq!(batches, [vec![8, 7], vec![6, 5], vec![4, 3], vec![2, 1]]);
    }

    #[test]
    fn test_iterate() {
        // Setup:
        // 4
        //   \
        //     -> 2 -> 1
        //   /
        // 3
        // 8 -> 7 -> 6 -> 5
        let (transaction_lookup_table, transaction_queue) = setup_test([
            (vec![4], vec![], vec![0]),
            (vec![3], vec![], vec![1]),
            (vec![2, 1], vec![], vec![0, 1]),
            (vec![8, 7, 6, 5], vec![], vec![2]),
        ]);
        let mut graph = PrioGraph::new(
            &transaction_lookup_table,
            transaction_queue,
            test_top_level_priority_fn,
        );
        assert!(!graph.is_empty());

        let account_write_locks = RefCell::new(HashMap::new());
        let scheduled = RefCell::new(Vec::new());
        let selector = |id: TxId, _: &GraphNode<TxId>| {
            let transaction = transaction_lookup_table.get(&id).expect("id must exist");
            let mut account_write_locks = account_write_locks.borrow_mut();
            let mut scheduled = scheduled.borrow_mut();

            let mut can_schedule = true;
            transaction.check_resource_keys(&mut |account: &TxId, access_kind: AccessKind| {
                if let AccessKind::Write = access_kind {
                    can_schedule &= !account_write_locks.contains_key(account);
                }
            });
            if can_schedule {
                transaction.check_resource_keys(&mut |account: &TxId, access_kind: AccessKind| {
                    if let AccessKind::Write = access_kind {
                        account_write_locks.insert(*account, id);
                    }
                });
                scheduled.push(id);
            }

            Selection {
                selected: if can_schedule {
                    SelectKind::SelectedNoBlock
                } else {
                    SelectKind::Unselected
                },
                continue_iterating: true,
            }
        };

        graph.iterate(selector);
        assert_eq!(*scheduled.borrow(), vec![8, 4, 3]);
        account_write_locks.borrow_mut().remove(&2); // remove the write-lock for account 2, unblocking 7.
        scheduled.borrow_mut().clear();

        graph.iterate(selector);
        assert_eq!(*scheduled.borrow(), vec![7]);
        account_write_locks.borrow_mut().clear();
        scheduled.borrow_mut().clear();

        graph.iterate(selector);
        assert_eq!(*scheduled.borrow(), vec![6, 2]);
        account_write_locks.borrow_mut().clear();
        scheduled.borrow_mut().clear();

        graph.iterate(selector);
        assert_eq!(*scheduled.borrow(), vec![5, 1]);
        assert!(graph.is_empty());
    }
}
