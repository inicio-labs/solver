use super::types::ClearingError;

pub const PPM_DENOMINATOR: u32 = 1_000_000;
pub const DEFAULT_MAX_ORDERS_PER_SIDE: usize = 100;
pub const DEFAULT_MAX_TOTAL_INTERVALS_PER_SIDE: usize = 50_000;

#[derive(Clone, Copy, Debug)]
pub struct ClearingConfig {
    pub protocol_fee_ppm: u32,
    pub max_orders_per_side: usize,
    pub max_total_intervals: usize,
}

impl ClearingConfig {
    pub(crate) fn validate(&self) -> Result<(), ClearingError> {
        if self.protocol_fee_ppm >= PPM_DENOMINATOR
            || self.max_orders_per_side == 0
            || self.max_total_intervals == 0
        {
            return Err(ClearingError::InvalidConfig);
        }
        Ok(())
    }
}

impl Default for ClearingConfig {
    fn default() -> Self {
        Self {
            protocol_fee_ppm: 0,
            max_orders_per_side: DEFAULT_MAX_ORDERS_PER_SIDE,
            max_total_intervals: DEFAULT_MAX_TOTAL_INTERVALS_PER_SIDE,
        }
    }
}
