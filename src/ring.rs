pub const LAMBDA_RING_CAP: usize = 4096;

pub struct LambdaRing {
    pub inner: [u64; LAMBDA_RING_CAP],
    pub head: usize,
    pub len: usize
}

impl LambdaRing {
    pub fn new() -> Self {
        LambdaRing {
            inner: [0u64; LAMBDA_RING_CAP],
            head: 0,
            len: 0,
        }
    }

    pub fn push(&mut self, ts: u64) {
        let idx = (self.head + self.len) & (LAMBDA_RING_CAP - 1);
        self.inner[idx] = ts;
        if self.len < LAMBDA_RING_CAP {
            self.len += 1;
        } else {
            self.head = (self.head + 1) & (LAMBDA_RING_CAP - 1);
        }
    }

    pub fn reset(&mut self, cutoff_ts: u64) {
        while self.len > 0 && self.inner[self.head] < cutoff_ts {
            self.head = (self.head + 1) & (LAMBDA_RING_CAP - 1);
            self.len -= 1;
        }
    }

    pub fn rate(&self, window_ns: u64) -> f64 {
        self.len as f64 / window_ns as f64 * 1e-9
    }
}