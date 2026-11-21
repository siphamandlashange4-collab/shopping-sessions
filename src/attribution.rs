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
        totals: &mut FxHashMap<(u32, i32), CampaignTotals>,
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
        let row = totals.entry((credited, day)).or_default();
        row.sessions += 1;
        row.purchasing_sessions += i64::from(revenue > 0);
        row.revenue += revenue;
        row.assisted_sessions += i64::from(credited != ORGANIC);

        for &campaign in &self.active {
            if campaign != credited {
                totals.entry((campaign, day)).or_default().assisted_sessions += 1;
            }
        }
    }
}
