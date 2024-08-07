use {
    criterion::{
        black_box, criterion_group, criterion_main, measurement::Measurement, BenchmarkGroup,
        Criterion,
    },
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
    let mut ids: Vec<_> = (0..num_transactions)
        .map(|id| TransactionPriorityId {
            id,
            priority: rng.sample(priority_distribution),
        })
        .collect();
    ids.sort_unstable();
    ids
}

fn bench_prio_graph(
    group: &mut BenchmarkGroup<impl Measurement>,
    name: &str,
    ids_and_txs: &[(TransactionPriorityId, TestTransaction)],
) {
    // Begin bench.
    let mut prio_graph = PrioGraph::new(|id, _| *id);
    group.bench_function(name, |bencher| {
        bencher.iter(|| {
            for (id, tx) in ids_and_txs.iter() {
                prio_graph.insert_transaction(*id, tx.resources());
            }
            let _batches = black_box(prio_graph.make_natural_batches());
            prio_graph.clear();
        });
    });
}

fn bench_prio_graph_random_access(
    group: &mut BenchmarkGroup<impl Measurement>,
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
        group,
        &format!("{num_transactions}_{num_accounts}_{num_accounts_per_transaction}"),
        &ids_and_txs,
    );
}

fn benchmark_prio_graph_random_access(bencher: &mut Criterion) {
    let mut group = bencher.benchmark_group("random_access");
    for num_transactions in [100, 1_000].iter().cloned() {
        for num_accounts in [100, 10_000].iter().cloned() {
            for num_accounts_per_transaction in [2, 64, 128].iter().cloned() {
                if num_accounts_per_transaction > num_accounts {
                    continue;
                }
                group.throughput(criterion::Throughput::Elements(num_transactions));
                bench_prio_graph_random_access(
                    &mut group,
                    num_transactions,
                    num_accounts,
                    num_accounts_per_transaction as usize,
                );
            }
        }
    }
}

fn bench_prio_graph_no_conflict(
    group: &mut BenchmarkGroup<impl Measurement>,
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
        group,
        &format!("{num_transactions}_{num_accounts_per_transaction}"),
        &ids_and_txs,
    );
}

fn benchmark_prio_graph_no_conflict(bencher: &mut Criterion) {
    let mut group = bencher.benchmark_group("no_conflict");
    for num_transactions in [100, 1_000].iter().cloned() {
        for num_accounts_per_transaction in [2, 64, 128].iter().cloned() {
            group.throughput(criterion::Throughput::Elements(num_transactions));
            bench_prio_graph_no_conflict(
                &mut group,
                num_transactions,
                num_accounts_per_transaction as usize,
            );
        }
    }
}

fn bench_prio_graph_all_conflict(
    group: &mut BenchmarkGroup<impl Measurement>,
    num_transactions: u64,
    num_accounts_per_transaction: usize,
) {
    // Generate priority-ordered ids
    let ids = generate_priority_ids(num_transactions);

    // Generate shared write accounts for all transactions.
    let write_accounts: Vec<_> = (0..num_accounts_per_transaction)
        .map(|_| AccountKey::random())
        .collect();

    // Generate transactions, store in vector with ids to avoid lookup.
    let ids_and_txs: Vec<_> = ids
        .iter()
        .map(|id| {
            (
                *id,
                TestTransaction {
                    read_accounts: vec![],
                    write_accounts: write_accounts.clone(),
                },
            )
        })
        .collect();

    bench_prio_graph(
        group,
        &format!("{num_transactions}_{num_accounts_per_transaction}"),
        &ids_and_txs,
    );
}

fn benchmark_prio_graph_all_conflict(bencher: &mut Criterion) {
    let mut group = bencher.benchmark_group("all_conflict");
    for num_transactions in [100, 1_000].iter().cloned() {
        for num_accounts_per_transaction in [2, 64, 128].iter().cloned() {
            group.throughput(criterion::Throughput::Elements(num_transactions));
            bench_prio_graph_all_conflict(
                &mut group,
                num_transactions,
                num_accounts_per_transaction as usize,
            );
        }
    }
}

fn bench_prio_graph_read_write(
    group: &mut BenchmarkGroup<impl Measurement>,
    num_transactions: u64,
    num_reads: usize,
) {
    // Generate priority-ordered ids
    let ids = generate_priority_ids(num_transactions);

    // A single shared account for all transactions.
    let accounts = vec![AccountKey::random()];

    // Generate transactions, store in vector with ids to avoid lookup.
    // Every num_reads + 1 transaction will write the shared account.
    let ids_and_txs: Vec<_> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let (read_accounts, write_accounts) = if i % (num_reads + 1) == 0 {
                (vec![], accounts.clone())
            } else {
                (accounts.clone(), vec![])
            };
            (
                *id,
                TestTransaction {
                    read_accounts,
                    write_accounts,
                },
            )
        })
        .collect();

    bench_prio_graph(
        group,
        &format!("{num_transactions}_{num_reads}"),
        &ids_and_txs,
    );
}

fn benchmark_prio_graph_read_write(bencher: &mut Criterion) {
    let mut group = bencher.benchmark_group("read_write");
    for num_transactions in [100, 1_000] {
        for num_reads in [1, 8, 16] {
            group.throughput(criterion::Throughput::Elements(num_transactions));
            bench_prio_graph_read_write(&mut group, num_transactions, num_reads);
        }
    }
}

fn bench_prio_graph_tree(
    group: &mut BenchmarkGroup<impl Measurement>,
    num_transactions: u64,
    num_trees: u64,
    branch_length: u64,
    num_accounts_per_transaction: usize,
) {
    // Check for valid input(s).
    assert!(num_transactions > 0);
    assert!(num_trees <= num_transactions);
    assert!(num_transactions % num_trees == 0);

    // Can calculate some properties of the tree-like structure.
    let num_transactions_per_tree = num_transactions / num_trees;
    let num_branches = num_transactions_per_tree / (1 + branch_length);
    let trunk_length = num_transactions_per_tree - num_branches * branch_length;
    let branch_spacing = trunk_length / num_branches;
    let concurrent_branches = branch_length / branch_spacing;

    // Sanity check for the shared account generation strategy.
    assert!(concurrent_branches <= num_accounts_per_transaction as u64,);

    // Generate priority ordered trees.
    let mut ids_and_txs = vec![];
    let mut unique_id = 0;
    for _ in 0..num_trees {
        // Constant shared write accounts for the main-trunk transactions.
        // Branches off of the main trunk, will share one of these accounts,
        // and the remainder will be used in a cycle.
        let mut write_accounts: Vec<_> = (0..num_accounts_per_transaction)
            .map(|_| AccountKey::random())
            .collect();
        let branch_accounts: Vec<Vec<_>> = (0..concurrent_branches)
            .map(|_| {
                // Generate the rest of the branch accounts.
                (0..(num_accounts_per_transaction - 1))
                    .map(|_| AccountKey::random())
                    .collect()
            })
            .collect();

        let mut remaining_transactions = num_transactions_per_tree;
        let mut branch_index = 0;
        let mut priority = u64::MAX;
        while remaining_transactions > 0 {
            // Generate the branch_spacing main-trunk transactions.
            for _ in 0..branch_spacing {
                remaining_transactions -= 1;

                let id = TransactionPriorityId {
                    id: unique_id,
                    priority,
                };
                unique_id += 1;
                priority -= 1;

                ids_and_txs.push((
                    id,
                    TestTransaction {
                        read_accounts: vec![],
                        write_accounts: write_accounts.clone(),
                    },
                ));
            }

            // Generate the branch transactions at this point (if remaining transactions).
            if remaining_transactions > 0 {
                // Cycle out the shared account for the branch.
                let account = write_accounts[branch_index];
                write_accounts[branch_index] = AccountKey::random();
                let mut branch_accounts = branch_accounts[branch_index].clone();
                branch_accounts.push(account);
                // Rotate branch index for the next time we branch.
                branch_index = (branch_index + 1) % concurrent_branches as usize;

                // Generate the branch_length transactions.
                for _ in 0..branch_length {
                    remaining_transactions -= 1;

                    let id = TransactionPriorityId {
                        id: unique_id,
                        priority,
                    };
                    unique_id += 1;
                    priority -= 1;

                    ids_and_txs.push((
                        id,
                        TestTransaction {
                            read_accounts: vec![],
                            write_accounts: branch_accounts.clone(),
                        },
                    ));
                }
            }
        }
    }

    bench_prio_graph(
        group,
        &format!("{num_transactions}_{branch_length}_{num_accounts_per_transaction}"),
        &ids_and_txs,
    );
}

fn benchmark_prio_graph_tree(bencher: &mut Criterion) {
    let mut group = bencher.benchmark_group("tree");
    for num_transactions in [100, 1_000].iter().cloned() {
        for num_accounts_per_transaction in [16, 64, 128].iter().cloned() {
            for branch_length in [4, 8].iter().cloned() {
                group.throughput(criterion::Throughput::Elements(num_transactions));
                bench_prio_graph_tree(
                    &mut group,
                    num_transactions,
                    1, // single tree
                    branch_length,
                    num_accounts_per_transaction as usize,
                );
            }
        }
    }
}

criterion_group!(random_access, benchmark_prio_graph_random_access);
criterion_group!(no_conflict, benchmark_prio_graph_no_conflict);
criterion_group!(all_conflict, benchmark_prio_graph_all_conflict);
criterion_group!(read_write, benchmark_prio_graph_read_write);
criterion_group!(tree, benchmark_prio_graph_tree);
criterion_main!(no_conflict, all_conflict, read_write, tree, random_access);
