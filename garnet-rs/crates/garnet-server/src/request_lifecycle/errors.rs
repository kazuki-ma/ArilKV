use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use tsavorite::{
    DeleteOperationError, PageManagerError, PageResidencyError, ReadOperationError,
    RmwOperationError, UpsertOperationError,
};

const GARNET_LOG_STORAGE_FAILURES_ENV: &str = "GARNET_LOG_STORAGE_FAILURES";
const STORAGE_FAILURE_LOG_LIMIT: usize = 64;
static STORAGE_FAILURE_LOG_ENABLED: OnceLock<bool> = OnceLock::new();
static STORAGE_FAILURE_LOG_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestExecutionError {
    WrongArity {
        command: &'static str,
        expected: &'static str,
    },
    UnknownCommand,
    NoSuchKey,
    NoGroup,
    SyntaxError,
    InvalidExpireTime,
    InvalidGetExExpireTime,
    WrongType,
    StorageBusy,
    StorageCapacityExceeded,
    StorageFailure,
    CommandDisabled {
        command: &'static str,
    },
    ValueNotInteger,
    ValueNotFloat,
    ValueOutOfRange,
    IndexOutOfRange,
    IncrementOverflow,
    AuthNotEnabled,
    DbIndexOutOfRange,
    SourceDestinationObjectsSame,
    WaitAofAppendOnlyDisabled,
    ClusterSupportDisabled,
}

impl RequestExecutionError {
    pub fn append_resp_error(self, response_out: &mut Vec<u8>) {
        match self {
            Self::WrongArity { command, .. } => append_error(
                response_out,
                &format!("ERR wrong number of arguments for '{}' command", command),
            ),
            Self::UnknownCommand => append_error(response_out, "ERR unknown command"),
            Self::NoSuchKey => append_error(response_out, "ERR no such key"),
            Self::NoGroup => append_error(response_out, "NOGROUP No such key or consumer group"),
            Self::SyntaxError => append_error(response_out, "ERR syntax error"),
            Self::InvalidExpireTime => {
                append_error(response_out, "ERR invalid expire time in 'set' command")
            }
            Self::InvalidGetExExpireTime => {
                append_error(response_out, "ERR invalid expire time in 'getex' command")
            }
            Self::WrongType => append_error(
                response_out,
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            ),
            Self::StorageBusy => append_error(response_out, "ERR storage busy, retry later"),
            Self::StorageCapacityExceeded => append_error(
                response_out,
                "ERR storage capacity exceeded (increase max in-memory pages)",
            ),
            Self::StorageFailure => append_error(response_out, "ERR internal storage failure"),
            Self::CommandDisabled { command } => append_error(
                response_out,
                &format!("ERR {} is disabled in this server", command),
            ),
            Self::ValueNotInteger => {
                append_error(response_out, "ERR value is not an integer or out of range")
            }
            Self::ValueNotFloat => append_error(response_out, "ERR value is not a valid float"),
            Self::ValueOutOfRange => append_error(response_out, "ERR value is out of range"),
            Self::IndexOutOfRange => append_error(response_out, "ERR index out of range"),
            Self::IncrementOverflow => {
                append_error(response_out, "ERR increment or decrement would overflow")
            }
            Self::AuthNotEnabled => append_error(
                response_out,
                "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?",
            ),
            Self::DbIndexOutOfRange => {
                append_error(response_out, "ERR DB index is out of range")
            }
            Self::SourceDestinationObjectsSame => append_error(
                response_out,
                "ERR source and destination objects are the same",
            ),
            Self::WaitAofAppendOnlyDisabled => append_error(
                response_out,
                "ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.",
            ),
            Self::ClusterSupportDisabled => append_error(
                response_out,
                "ERR This instance has cluster support disabled",
            ),
        }
    }
}

fn storage_failure_logging_enabled() -> bool {
    *STORAGE_FAILURE_LOG_ENABLED.get_or_init(|| {
        std::env::var(GARNET_LOG_STORAGE_FAILURES_ENV)
            .map(|value| !matches!(value.as_str(), "0" | "false" | "FALSE"))
            .unwrap_or(true)
    })
}

fn log_storage_failure(context: &str, detail: &str) {
    if !storage_failure_logging_enabled() {
        return;
    }

    let count = STORAGE_FAILURE_LOG_COUNT.fetch_add(1, Ordering::Relaxed);
    if count >= STORAGE_FAILURE_LOG_LIMIT {
        if count == STORAGE_FAILURE_LOG_LIMIT {
            eprintln!(
                "garnet-server storage failure logging suppressed after {} entries",
                STORAGE_FAILURE_LOG_LIMIT
            );
        }
        return;
    }

    let backtrace = std::backtrace::Backtrace::force_capture();
    eprintln!(
        "garnet-server storage failure [{}]: {}\nbacktrace:\n{}",
        context, detail, backtrace
    );
}

pub(super) fn storage_failure(context: &str, detail: &str) -> RequestExecutionError {
    log_storage_failure(context, detail);
    RequestExecutionError::StorageFailure
}

pub(super) fn map_read_error(error: ReadOperationError) -> RequestExecutionError {
    match error {
        ReadOperationError::PageManager(PageManagerError::BufferFull { .. }) => {
            RequestExecutionError::StorageCapacityExceeded
        }
        ReadOperationError::PageResidency(PageResidencyError::PageManager(
            PageManagerError::BufferFull { .. },
        )) => RequestExecutionError::StorageCapacityExceeded,
        ReadOperationError::PageResidency(PageResidencyError::NoEvictablePage { .. }) => {
            RequestExecutionError::StorageBusy
        }
        other => storage_failure("read", &format!("{other:?}")),
    }
}

pub(super) fn map_upsert_error(error: UpsertOperationError) -> RequestExecutionError {
    match error {
        UpsertOperationError::PageManager(PageManagerError::BufferFull { .. }) => {
            RequestExecutionError::StorageCapacityExceeded
        }
        UpsertOperationError::CompareExchangeConflict => RequestExecutionError::StorageBusy,
        other => storage_failure("upsert", &format!("{other:?}")),
    }
}

pub(super) fn map_delete_error(error: DeleteOperationError) -> RequestExecutionError {
    match error {
        DeleteOperationError::PageManager(PageManagerError::BufferFull { .. }) => {
            RequestExecutionError::StorageCapacityExceeded
        }
        DeleteOperationError::CompareExchangeConflict => RequestExecutionError::StorageBusy,
        other => storage_failure("delete", &format!("{other:?}")),
    }
}

pub(super) fn map_rmw_error(error: RmwOperationError) -> RequestExecutionError {
    match error {
        RmwOperationError::PageManager(PageManagerError::BufferFull { .. }) => {
            RequestExecutionError::StorageCapacityExceeded
        }
        RmwOperationError::CompareExchangeConflict => RequestExecutionError::StorageBusy,
        other => storage_failure("rmw", &format!("{other:?}")),
    }
}

fn append_error(response_out: &mut Vec<u8>, message: &str) {
    response_out.push(b'-');
    response_out.extend_from_slice(message.as_bytes());
    response_out.extend_from_slice(b"\r\n");
}
