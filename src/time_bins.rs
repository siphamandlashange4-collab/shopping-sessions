const SESSION_GAP_MS: i64 = 1_800_000;
const MAX_BINS: usize = 4096;
const MAX_CATEGORY_CELLS: usize = 131_072;

#[derive(Clone, Copy)]
struct Bin {
    first: i64,
    last: i64,
    events: i64,
    revenue: i64,
}

impl Bin {
    const EMPTY: Self = Self {
        first: i64::MAX,
        last: i64::MIN,
        events: 0,
        revenue: 0,
    };
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SessionSummary {
    pub start: i64,
    pub events: i64,
    pub revenue: i64,
}

pub struct SessionBins {
    start: i64,
    bins: Vec<Bin>,
    category_count: usize,
    bin_revenue: Vec<i64>,
    session_revenue: Vec<i64>,
}

impl SessionBins {
    pub fn new(start: i64, end: i64, categories: usize) -> Option<Self> {
        let duration = end.checked_sub(start)?;
        if duration <= 0 {
            return None;
        }
        let count = usize::try_from((duration - 1) / SESSION_GAP_MS + 1).ok()?;
        let cells = count.checked_mul(categories)?;
        if count > MAX_BINS || cells > MAX_CATEGORY_CELLS {
            return None;
        }
        Some(Self {
            start,
            bins: vec![Bin::EMPTY; count],
            category_count: categories,
            bin_revenue: vec![0; cells],
            session_revenue: vec![0; categories],
        })
    }

    /// Sessionize the events of one person in the configured timestamp range.
    /// Every pair in a bin is at most 30 minutes apart. Across nonempty bins
    /// only the distance from the previous maximum to the next minimum matters.
    pub fn summarize<F: Copy>(
        &mut self,
        events: &[F],
        time: impl Fn(F) -> i64,
        purchase: impl Fn(F) -> (u32, u32),
        mut emit: impl FnMut(SessionSummary, &[i64]),
    ) {
        self.bins.fill(Bin::EMPTY);
        self.bin_revenue.fill(0);
        self.session_revenue.fill(0);

        for &event in events {
            let time = time(event);
            let index = ((time - self.start) / SESSION_GAP_MS) as usize;
            let bin = &mut self.bins[index];
            bin.first = bin.first.min(time);
            bin.last = bin.last.max(time);
            bin.events += 1;
            let (price, category) = purchase(event);
            if price != 0 {
                let price = i64::from(price);
                bin.revenue += price;
                self.bin_revenue[index * self.category_count + category as usize] += price;
            }
        }

        let mut session = SessionSummary::default();
        let mut previous = 0;
        for (index, bin) in self.bins.iter().enumerate() {
            if bin.events == 0 {
                continue;
            }
            if session.events != 0 && bin.first - previous > SESSION_GAP_MS {
                emit(session, &self.session_revenue);
                session = SessionSummary::default();
                self.session_revenue.fill(0);
            }
            if session.events == 0 {
                session.start = bin.first;
            }
            previous = bin.last;
            session.events += bin.events;
            session.revenue += bin.revenue;
            if bin.revenue > 0 {
                let offset = index * self.category_count;
                for (session_total, &bin_total) in self
                    .session_revenue
                    .iter_mut()
                    .zip(&self.bin_revenue[offset..offset + self.category_count])
                {
                    *session_total += bin_total;
                }
            }
        }
        if session.events != 0 {
            emit(session, &self.session_revenue);
        }
    }
}
