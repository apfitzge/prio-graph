use {
    criterion::{black_box, criterion_group, criterion_main, Criterion},
    prio_graph::{AccessKind, PrioGraph, TopLevelId},
    rand::{distributions::Uniform, seq::SliceRandom, thread_rng, Rng},
    std::{collections::HashMap, fmt::Display, hash::Hash},
};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct TransactionPriorityId {
    id: u64,
    priority: u64,
}

impl Hash for TransactionPriorityId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(self.id);
    }
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

impl TopLevelId<TransactionPriorityId> for TransactionPriorityId {
    fn id(&self) -> TransactionPriorityId {
        *self
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
struct AccountKey([u8; 32]);

struct TestTransaction {
    read_accounts: Vec<AccountKey>,
    write_accounts: Vec<AccountKey>,
}

impl TestTransaction {
    fn resources(&self) -> impl Iterator<Item = (AccountKey, AccessKind)> + '_ {
        let write_locked_resources = self
            .write_accounts
            .iter()
            .cloned()
            .map(|rk| (rk, AccessKind::Write));
        let read_locked_resources = self
            .read_accounts
            .iter()
            .cloned()
            .map(|rk| (rk, AccessKind::Read));

        write_locked_resources.chain(read_locked_resources)
    }
}

fn bench_prio_graph_random_access(
    bencher: &mut Criterion,
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
                    read_accounts: vec![],
                    write_accounts,
                },
            )
        })
        .collect();

    // Begin bench.
    bencher.bench_function(
        &format!("bench_prio_graph_random_access_{num_transactions}_txs_{num_accounts}_accts"),
        |bencher| {
            bencher.iter(|| {
                let _batches = black_box(PrioGraph::natural_batches(
                    ids.iter().cloned().map(|id| {
                        (
                            id,
                            transaction_lookup_table
                                .get(&id)
                                .expect("id must exist")
                                .resources(),
                        )
                    }),
                    |id, _| *id,
                ));
            });
        },
    );
}

fn benchmark_prio_graph_random_access(bencher: &mut Criterion) {
    bench_prio_graph_random_access(bencher, 100, 2);
    bench_prio_graph_random_access(bencher, 100, 100);
    bench_prio_graph_random_access(bencher, 100, 1_000);
    bench_prio_graph_random_access(bencher, 1_000, 2);
    bench_prio_graph_random_access(bencher, 1_000, 100);
    bench_prio_graph_random_access(bencher, 1_000, 1_000);
    bench_prio_graph_random_access(bencher, 1_000, 10_000);
    bench_prio_graph_random_access(bencher, 10_000, 2);
    bench_prio_graph_random_access(bencher, 10_000, 1_000);
    bench_prio_graph_random_access(bencher, 10_000, 10_000);
}

criterion_group!(random_access, benchmark_prio_graph_random_access);
criterion_main!(random_access);
