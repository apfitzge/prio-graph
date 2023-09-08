#![feature(test)]

extern crate test;

use {
    prio_graph::{AccessKind, PrioGraph, PriorityId, ResourceKey, Transaction},
    rand::{distributions::Uniform, seq::SliceRandom, thread_rng, Rng},
    std::{collections::HashMap, fmt::Display},
    test::Bencher,
};

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
struct TransactionPriorityId {
    id: u64,
    priority: u64,
}

impl Ord for TransactionPriorityId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.priority.cmp(&other.priority) {
            std::cmp::Ordering::Equal => self.id.cmp(&other.id),
            other => other,
        }
    }
}

impl PartialOrd for TransactionPriorityId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Display for TransactionPriorityId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(id: {}, prio: {})", self.id, self.priority)
    }
}

impl PriorityId for TransactionPriorityId {}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
struct AccountKey([u8; 32]);

impl ResourceKey for AccountKey {}

struct TestTransaction {
    id: TransactionPriorityId,
    read_accounts: Vec<AccountKey>,
    write_accounts: Vec<AccountKey>,
}

impl Transaction<TransactionPriorityId, AccountKey> for TestTransaction {
    fn id(&self) -> TransactionPriorityId {
        self.id
    }

    fn check_resource_keys<F: FnMut(&AccountKey, AccessKind)>(&self, mut checker: F) {
        for account in &self.read_accounts {
            checker(account, AccessKind::Read);
        }
        for account in &self.write_accounts {
            checker(account, AccessKind::Write);
        }
    }
}

fn bench_prio_graph_build_and_consume(
    bencher: &mut Bencher,
    num_transactions: u64,
    num_accounts: u64,
) {
    let mut rng = thread_rng();
    let priority_distribution = Uniform::new(0, 1000);

    // Generate priority-ordered ids
    let ids = {
        let mut ids: Vec<_> = (0..num_transactions)
            .rev()
            .map(|id| (id, rng.sample(priority_distribution)))
            .map(|(id, priority)| TransactionPriorityId { id, priority })
            .collect();

        // Sort in reverse order so that highest priority are at the top.
        ids.sort_by(|a, b| b.priority.cmp(&a.priority));
        ids
    };

    // Generate account keys.
    let account_keys: Vec<_> = (0..num_accounts)
        .map(|_| {
            let mut key = [0u8; 32];
            rng.fill(&mut key[..]);
            AccountKey(key)
        })
        .collect();

    // Generate transactions, store in lookup table.
    let num_accounts_distribution = Uniform::new(2, 32);
    let transaction_lookup_table: HashMap<_, _> = ids
        .iter()
        .map(|id| {
            let transaction_num_accounts = rng.sample(num_accounts_distribution);

            // Assume all write-accounts for now.
            let write_accounts = account_keys
                .choose_multiple(&mut rng, transaction_num_accounts as usize)
                .cloned()
                .collect();
            (
                *id,
                TestTransaction {
                    id: *id,
                    read_accounts: vec![],
                    write_accounts,
                },
            )
        })
        .collect();

    // Begin bench.
    bencher.iter(|| {
        let prio_graph = test::black_box(PrioGraph::new(
            &transaction_lookup_table,
            ids.iter().cloned(),
        ));
        let _batches = test::black_box(prio_graph.natural_batches());
    });
}

#[bench]
fn test_small_graph(bencher: &mut Bencher) {
    bench_prio_graph_build_and_consume(bencher, 100, 100);
}

#[bench]
fn test_medium_graph(bencher: &mut Bencher) {
    bench_prio_graph_build_and_consume(bencher, 10_000, 10_000);
}
