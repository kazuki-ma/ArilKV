use garnet_cluster::SlotNumber;
use garnet_common::ArgSlice;
use garnet_common::RespParseError;
use garnet_common::parse_resp_command_arg_slices_dynamic;
use std::sync::Arc;

use crate::ClientId;
use crate::RequestProcessor;
use crate::ShardOwnerThreadPool;
use crate::command_spec::CommandId;
use crate::connection_protocol::ascii_eq_ignore_case;
use crate::connection_protocol::parse_u16_ascii;
use crate::connection_routing::owner_shard_for_command;
use crate::dispatch_from_arg_slices;
use crate::request_lifecycle::DbName;
use crate::request_lifecycle::RedisKey;
use crate::request_lifecycle::ShardIndex;
use crate::request_lifecycle::WatchVersion;
use crate::request_lifecycle::WatchedKey;

#[derive(Default)]
pub(crate) struct ConnectionTransactionState {
    pub(crate) in_multi: bool,
    pub(crate) queued_frames: Vec<Vec<u8>>,
    pub(crate) watched_keys: Vec<WatchedKey>,
    pub(crate) transaction_slot: Option<SlotNumber>,
    pub(crate) aborted: bool,
    pub(crate) aborted_due_to_busy_script: bool,
}

impl ConnectionTransactionState {
    pub(crate) fn reset(&mut self) {
        self.in_multi = false;
        self.queued_frames.clear();
        self.watched_keys.clear();
        self.transaction_slot = None;
        self.aborted = false;
        self.aborted_due_to_busy_script = false;
    }

    pub(crate) fn clear_watches(&mut self) {
        self.watched_keys.clear();
    }

    pub(crate) fn watch_key(&mut self, db: DbName, key: &[u8], version: WatchVersion) {
        if let Some(watched) = self
            .watched_keys
            .iter_mut()
            .find(|watched| watched.db == db && watched.key.as_slice() == key)
        {
            watched.version = version;
            return;
        }
        self.watched_keys
            .push(WatchedKey::new(db, RedisKey::from(key), version));
    }

    pub(crate) fn set_transaction_slot_or_abort(&mut self, slot: SlotNumber) -> bool {
        match self.transaction_slot {
            None => {
                self.transaction_slot = Some(slot);
                true
            }
            Some(existing) if existing == slot => true,
            Some(_) => {
                self.aborted = true;
                false
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueuedReplicationTransition {
    BecomeMaster,
    BecomeReplica { host: String, port: u16 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExecutedTransactionItem {
    pub(crate) selected_db: DbName,
    pub(crate) frame: Vec<u8>,
    pub(crate) response: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TransactionExecutionOutcome {
    pub(crate) items: Vec<ExecutedTransactionItem>,
    pub(crate) selected_db_after_exec: DbName,
    pub(crate) pending_replication_transition: Option<QueuedReplicationTransition>,
}

pub(crate) fn execute_transaction_queue(
    processor: &Arc<RequestProcessor>,
    owner_thread_pool: &Arc<ShardOwnerThreadPool>,
    transaction: &mut ConnectionTransactionState,
    responses: &mut Vec<u8>,
    max_resp_arguments: usize,
    client_no_touch: bool,
    client_id: Option<ClientId>,
    selected_db: DbName,
) -> TransactionExecutionOutcome {
    let queued = std::mem::take(&mut transaction.queued_frames);
    transaction.in_multi = false;
    transaction.watched_keys.clear();
    transaction.transaction_slot = None;
    transaction.aborted = false;
    transaction.aborted_due_to_busy_script = false;

    let owner_shard = transaction_owner_shard(processor, &queued, max_resp_arguments)
        .unwrap_or(ShardIndex::new(0));
    let queued_len = queued.len();
    let routed_processor = Arc::clone(processor);
    let TransactionExecutionOutcome {
        items,
        selected_db_after_exec,
        pending_replication_transition,
    } = owner_thread_pool
        .execute_sync(owner_shard, move || {
            execute_transaction_queue_on_owner_thread(
                &routed_processor,
                queued,
                max_resp_arguments,
                client_no_touch,
                client_id,
                selected_db,
            )
        })
        .unwrap_or_else(|_| TransactionExecutionOutcome {
            items: vec![
                ExecutedTransactionItem {
                    selected_db,
                    frame: Vec::new(),
                    response: b"-ERR owner routing execution failed\r\n".to_vec(),
                };
                queued_len
            ],
            selected_db_after_exec: selected_db,
            pending_replication_transition: None,
        });

    responses.push(b'*');
    responses.extend_from_slice(items.len().to_string().as_bytes());
    responses.extend_from_slice(b"\r\n");
    for item in &items {
        responses.extend_from_slice(&item.response);
    }

    TransactionExecutionOutcome {
        items,
        selected_db_after_exec,
        pending_replication_transition,
    }
}

fn transaction_owner_shard(
    processor: &Arc<RequestProcessor>,
    queued: &[Vec<u8>],
    max_resp_arguments: usize,
) -> Option<ShardIndex> {
    let mut args = vec![ArgSlice::EMPTY; 64.min(max_resp_arguments.max(1))];
    for frame in queued {
        match parse_resp_command_arg_slices_dynamic(frame, &mut args, max_resp_arguments) {
            Ok(meta) if meta.bytes_consumed == frame.len() => {
                // SAFETY: parsed ArgSlice values reference bytes in `frame` for this scope.
                let command = unsafe { dispatch_from_arg_slices(&args[..meta.argument_count]) };
                if matches!(command, CommandId::Select)
                    || command_is_replication_passthrough(arg_slice_bytes(&args[0]))
                {
                    continue;
                }
                return Some(owner_shard_for_command(
                    processor,
                    &args[..meta.argument_count],
                    command,
                ));
            }
            _ => continue,
        }
    }
    queued.first().and_then(|frame| {
        match parse_resp_command_arg_slices_dynamic(frame, &mut args, max_resp_arguments) {
            Ok(meta) if meta.bytes_consumed == frame.len() => {
                // SAFETY: parsed ArgSlice values reference bytes in `frame` for this scope.
                let command = unsafe { dispatch_from_arg_slices(&args[..meta.argument_count]) };
                Some(owner_shard_for_command(
                    processor,
                    &args[..meta.argument_count],
                    command,
                ))
            }
            _ => None,
        }
    })
}

fn execute_transaction_queue_on_owner_thread(
    processor: &RequestProcessor,
    queued: Vec<Vec<u8>>,
    max_resp_arguments: usize,
    client_no_touch: bool,
    client_id: Option<ClientId>,
    selected_db: DbName,
) -> TransactionExecutionOutcome {
    let mut args = vec![ArgSlice::EMPTY; 64.min(max_resp_arguments.max(1))];
    let mut items = Vec::with_capacity(queued.len());
    let mut pending_replication_transition = None;
    let mut transaction_selected_db = selected_db;
    processor.begin_tracking_invalidation_batch();
    for frame in queued {
        let mut item_response = Vec::new();
        let item_selected_db = transaction_selected_db;
        match parse_resp_command_arg_slices_dynamic(&frame, &mut args, max_resp_arguments) {
            Ok(meta) if meta.bytes_consumed == frame.len() => {
                let command_name = arg_slice_bytes(&args[0]);
                if command_is_replication_passthrough(command_name) {
                    match parse_replication_transition(&args[..meta.argument_count]) {
                        Ok(transition) => {
                            pending_replication_transition = Some(transition);
                            item_response.extend_from_slice(b"+OK\r\n");
                        }
                        Err(ReplicationTransitionParseError::WrongArity { command_name_upper }) => {
                            item_response
                                .extend_from_slice(b"-ERR wrong number of arguments for '");
                            item_response.extend_from_slice(command_name_upper);
                            item_response.extend_from_slice(b"' command\r\n");
                        }
                        Err(ReplicationTransitionParseError::InvalidPort) => {
                            item_response.extend_from_slice(
                                b"-ERR value is not an integer or out of range\r\n",
                            );
                        }
                    }
                    items.push(ExecutedTransactionItem {
                        selected_db: item_selected_db,
                        frame,
                        response: item_response,
                    });
                    continue;
                }
                // SAFETY: parsed ArgSlice values reference bytes in `frame` for this scope.
                let command = unsafe { dispatch_from_arg_slices(&args[..meta.argument_count]) };
                match processor.execute_with_client_no_touch_in_transaction_in_db(
                    &args[..meta.argument_count],
                    &mut item_response,
                    client_no_touch,
                    client_id,
                    transaction_selected_db,
                ) {
                    Ok(()) => {
                        if command == CommandId::Select
                            && let Some(next_db) =
                                parse_transaction_select_db(&args[..meta.argument_count], processor)
                        {
                            transaction_selected_db = next_db;
                        }
                    }
                    Err(error) => error.append_resp_error(&mut item_response),
                }
            }
            Err(RespParseError::ArgumentCapacityExceeded { .. }) => {
                item_response.extend_from_slice(b"-ERR too many arguments in request\r\n");
            }
            _ => item_response.extend_from_slice(b"-ERR protocol error\r\n"),
        }
        items.push(ExecutedTransactionItem {
            selected_db: item_selected_db,
            frame,
            response: item_response,
        });
    }
    processor.finish_tracking_invalidation_batch(client_id);
    TransactionExecutionOutcome {
        items,
        selected_db_after_exec: transaction_selected_db,
        pending_replication_transition,
    }
}

#[inline]
fn arg_slice_bytes(arg: &ArgSlice) -> &[u8] {
    // SAFETY: transaction queue parser stores `ArgSlice` values pointing into
    // the currently processed queued frame, which is alive for this scope.
    unsafe { arg.as_slice() }
}

fn command_is_replication_passthrough(command_name: &[u8]) -> bool {
    ascii_eq_ignore_case(command_name, b"REPLICAOF")
        || ascii_eq_ignore_case(command_name, b"SLAVEOF")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicationTransitionParseError {
    WrongArity { command_name_upper: &'static [u8] },
    InvalidPort,
}

fn parse_replication_transition(
    args: &[ArgSlice],
) -> Result<QueuedReplicationTransition, ReplicationTransitionParseError> {
    if args.is_empty() {
        return Err(ReplicationTransitionParseError::WrongArity {
            command_name_upper: b"REPLICAOF",
        });
    }
    let command_name = arg_slice_bytes(&args[0]);
    if args.len() != 3 {
        return Err(ReplicationTransitionParseError::WrongArity {
            command_name_upper: if ascii_eq_ignore_case(command_name, b"SLAVEOF") {
                b"SLAVEOF"
            } else {
                b"REPLICAOF"
            },
        });
    }

    let host_or_no = arg_slice_bytes(&args[1]);
    let port_or_one = arg_slice_bytes(&args[2]);
    if ascii_eq_ignore_case(host_or_no, b"NO") && ascii_eq_ignore_case(port_or_one, b"ONE") {
        return Ok(QueuedReplicationTransition::BecomeMaster);
    }

    let Some(port) = parse_u16_ascii(port_or_one) else {
        return Err(ReplicationTransitionParseError::InvalidPort);
    };

    Ok(QueuedReplicationTransition::BecomeReplica {
        host: String::from_utf8_lossy(host_or_no).to_string(),
        port,
    })
}

fn parse_transaction_select_db(args: &[ArgSlice], processor: &RequestProcessor) -> Option<DbName> {
    if args.len() != 2 {
        return None;
    }
    let raw = std::str::from_utf8(arg_slice_bytes(&args[1])).ok()?;
    let index = raw.parse::<usize>().ok()?;
    if index >= processor.configured_databases() {
        return None;
    }
    Some(DbName::new(index))
}
