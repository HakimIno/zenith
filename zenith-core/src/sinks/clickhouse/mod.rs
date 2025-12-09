//! ClickHouse sink implementation

pub mod native;
pub mod migrator;

pub use native::ClickHouseSink;
pub use migrator::ClickHouseMigrator;

