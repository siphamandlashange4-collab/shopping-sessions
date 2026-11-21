use rustc_hash::FxHashMap;
use std::ops::AddAssign;

const SEVEN_DAYS_MS: i64 = 7 * 86_400_000;
const ORGANIC: u32 = 0;

pub struct Click {
    pub user: u32,
    pub campaign: u32,
    pub time: i64,
    pub id: &'static [u8],
}

#[derive(Default)]
pub struct CampaignTotals {
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue: i64,
    pub assisted_sessions: i64,
}

impl AddAssign for CampaignTotals {
    fn add_assign(&mut self, other: Self) {
        self.sessions += other.sessions;
        self.purchasing_sessions += other.purchasing_sessions;
        self.revenue += other.revenue;
        self.assisted_sessions += other.assisted_sessions;
    }
}

pub struct CampaignTable {
    storage: CampaignStorage,
}

enum CampaignStorage {
    Dense {
        first_day: i32,
        campaigns: usize,
        values: Vec<CampaignTotals>,
    },
    Sparse(FxHashMap<(u32, i32), CampaignTotals>),
}

impl CampaignTable {
    pub fn new(campaigns: usize, first_day: i32, last_day: i32) -> Self {
        // A large valid window or catalog should not require preallocating the
        // full Cartesian product of days and campaigns in every worker.
        const MAX_DENSE_ROWS: usize = 65_536;
        let day_count = i64::from(last_day) - i64::from(first_day) + 1;
        let row_count = usize::try_from(day_count)
            .ok()
            .and_then(|days| days.checked_mul(campaigns));
        let storage = match row_count {
            Some(rows @ 1..=MAX_DENSE_ROWS) => CampaignStorage::Dense {
                first_day,
                campaigns,
                values: std::iter::repeat_with(CampaignTotals::default)
                    .take(rows)
                    .collect(),
            },
            _ => CampaignStorage::Sparse(FxHashMap::default()),
        };
        Self { storage }
    }

    fn record(&mut self, credited: u32, day: i32, revenue: i64, assisted: &[u32]) {
        match &mut self.storage {
            CampaignStorage::Dense {
                first_day,
                campaigns,
                values,
            } => {
                let offset = (day - *first_day) as usize * *campaigns;
                let today = &mut values[offset..offset + *campaigns];
                record_credit(&mut today[credited as usize], revenue);
                for &campaign in assisted {
                    today[campaign as usize].assisted_sessions += 1;
                }
            }
            CampaignStorage::Sparse(values) => {
                let row = values.entry((credited, day)).or_default();
                record_credit(row, revenue);
                row.assisted_sessions += i64::from(credited != ORGANIC);
                for &campaign in assisted {
                    if campaign != credited {
                        values.entry((campaign, day)).or_default().assisted_sessions += 1;
                    }
                }
            }
        }
    }

    /// Merge a shard created with the same campaign catalog and date bounds.
    pub fn merge(&mut self, other: Self) {
        match (&mut self.storage, other.storage) {
            (
                CampaignStorage::Dense {
                    first_day,
                    campaigns,
                    values,
                },
                CampaignStorage::Dense {
                    first_day: other_day,
                    campaigns: other_campaigns,
                    values: other_values,
                },
            ) => {
                assert_eq!(
                    (*first_day, *campaigns, values.len()),
                    (other_day, other_campaigns, other_values.len())
                );
                for (total, partial) in values.iter_mut().zip(other_values) {
                    *total += partial;
                }
            }
            (CampaignStorage::Sparse(values), CampaignStorage::Sparse(other_values)) => {
                for (key, partial) in other_values {
                    *values.entry(key).or_default() += partial;
                }
            }
            _ => unreachable!("campaign tables use the same bounds"),
        }
    }

    pub fn into_rows(self) -> FxHashMap<(u32, i32), CampaignTotals> {
        match self.storage {
            CampaignStorage::Dense {
                first_day,
                campaigns,
                values,
            } => values
                .into_iter()
                .enumerate()
                .filter(|(_, totals)| totals.sessions != 0 || totals.assisted_sessions != 0)
                .map(|(index, totals)| {
                    let day = (i64::from(first_day) + (index / campaigns) as i64) as i32;
                    (((index % campaigns) as u32, day), totals)
                })
                .collect(),
            CampaignStorage::Sparse(values) => values,
        }
    }
}

#[inline]
fn record_credit(row: &mut CampaignTotals, revenue: i64) {
    row.sessions += 1;
    row.purchasing_sessions += i64::from(revenue > 0);
    row.revenue += revenue;
}

pub struct Attribution {
    left: usize,
    right: usize,
    counts: Vec<usize>,
    positions: Vec<usize>,
    active: Vec<u32>,
}

impl Attribution {
    pub fn new(campaigns: usize) -> Self {
        Self {
            left: 0,
            right: 0,
            counts: vec![0; campaigns],
            positions: vec![0; campaigns],
            active: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        for campaign in self.active.drain(..) {
            self.counts[campaign as usize] = 0;
        }
        self.left = 0;
        self.right = 0;
    }

    /// Record one of this person's sessions in time order. The click slice stays
    /// fixed until reset and sorts by time, with descending IDs at equal times.
    pub fn record(
        &mut self,
        clicks: &[Click],
        start: i64,
        day: i32,
        revenue: i64,
        totals: &mut CampaignTable,
    ) {
        let earliest = start.saturating_sub(SEVEN_DAYS_MS);
        while self.left < self.right && clicks[self.left].time < earliest {
            let campaign = clicks[self.left].campaign as usize;
            self.counts[campaign] -= 1;
            if self.counts[campaign] == 0 {
                let removed = self.positions[campaign];
                self.active.swap_remove(removed);
                if let Some(&moved) = self.active.get(removed) {
                    self.positions[moved as usize] = removed;
                }
            }
            self.left += 1;
        }

        // When no earlier click remains active, skip newly encountered clicks
        // that are already too old for this session and every later session.
        if self.left == self.right {
            while self.right < clicks.len() && clicks[self.right].time < earliest {
                self.right += 1;
            }
            self.left = self.right;
        }

        while let Some(click) = clicks.get(self.right) {
            if click.time > start {
                break;
            }
            let campaign = click.campaign as usize;
            if self.counts[campaign] == 0 {
                self.positions[campaign] = self.active.len();
                self.active.push(click.campaign);
            }
            self.counts[campaign] += 1;
            self.right += 1;
        }

        let credited = if self.left < self.right {
            clicks[self.right - 1].campaign
        } else {
            ORGANIC
        };
        totals.record(credited, day, revenue, &self.active);
    }
}
