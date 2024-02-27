use {
    criterion::{black_box, criterion_group, criterion_main, Criterion},
    prio_graph::{AccessKind, PrioGraph, TopLevelId},
    rand::{distributions::Uniform, seq::SliceRandom, thread_rng, Rng},
    std::{fmt::Display, hash::Hash},
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

impl AccountKey {
    fn random() -> Self {
        let mut rng = thread_rng();
        let mut key = [0u8; 32];
        rng.fill(&mut key[..]);
        AccountKey(key)
    }
}

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

fn generate_priority_ids(num_transactions: u64) -> Vec<TransactionPriorityId> {
    let mut rng = thread_rng();
    let priority_distribution = Uniform::new(0, 1000);
    (0..num_transactions)
        .map(|id| TransactionPriorityId {
            id,
            priority: rng.sample(priority_distribution),
        })
        .collect()
}

fn bench_prio_graph(
    bencher: &mut Criterion,
    name: &str,
    ids_and_txs: &[(TransactionPriorityId, TestTransaction)],
) {
    // Begin bench.
    bencher.bench_function(name, |bencher| {
        bencher.iter(|| {
            let _batches = black_box(PrioGraph::natural_batches(
                ids_and_txs.iter().map(|(id, tx)| (*id, tx.resources())),
                |id, _| *id,
            ));
        });
    });
}

fn bench_prio_graph_random_access(
    bencher: &mut Criterion,
    num_transactions: u64,
    num_accounts: u64,
    num_accounts_per_transaction: usize,
) {
    // Generate priority-ordered ids
    let ids = generate_priority_ids(num_transactions);

    // Generate account keys.
    let account_keys: Vec<_> = (0..num_accounts).map(|_| AccountKey::random()).collect();

    // Generate transactions, store in vector with ids to avoid lookup.
    let mut rng = thread_rng();
    let ids_and_txs: Vec<_> = ids
        .iter()
        .map(|id| {
            // Assume all write-accounts for now.
            let write_accounts = account_keys
                .choose_multiple(&mut rng, num_accounts_per_transaction)
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

    bench_prio_graph(
        bencher,
        &format!("random_access_{num_transactions}_{num_accounts}_{num_accounts_per_transaction}"),
        &ids_and_txs,
    );
}

fn benchmark_prio_graph_random_access(bencher: &mut Criterion) {
    for num_transactions in [100, 1_000, 10_000].iter().cloned() {
        for num_accounts in [2, 100, 1_000, 10_000].iter().cloned() {
            for num_accounts_per_transaction in [2, 16, 32, 64, 128, 256].iter().cloned() {
                if num_accounts_per_transaction > num_accounts {
                    continue;
                }
                bench_prio_graph_random_access(
                    bencher,
                    num_transactions,
                    num_accounts,
                    num_accounts_per_transaction as usize,
                );
            }
        }
    }
}

fn bench_prio_graph_no_conflict(
    bencher: &mut Criterion,
    num_transactions: u64,
    num_accounts_per_transaction: usize,
) {
    // Generate priority-ordered ids
    let ids = generate_priority_ids(num_transactions);

    // Generate transactions, store in vector with ids to avoid lookup.
    let ids_and_txs: Vec<_> = ids
        .iter()
        .map(|id| {
            // Generate unique write-accounts for each.
            let write_accounts: Vec<_> = (0..num_accounts_per_transaction)
                .map(|_| AccountKey::random())
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

    bench_prio_graph(
        bencher,
        &format!("no_conflict_{num_transactions}_{num_accounts_per_transaction}"),
        &ids_and_txs,
    );
}

fn benchmark_prio_graph_no_conflict(bencher: &mut Criterion) {
    for num_transactions in [100, 1_000, 10_000].iter().cloned() {
        for num_accounts_per_transaction in [2, 16, 32, 64, 128, 256].iter().cloned() {
            bench_prio_graph_no_conflict(
                bencher,
                num_transactions,
                num_accounts_per_transaction as usize,
            );
        }
    }
}

criterion_group!(random_access, benchmark_prio_graph_random_access);
criterion_group!(no_conflict, benchmark_prio_graph_no_conflict);
criterion_main!(random_access, no_conflict);
