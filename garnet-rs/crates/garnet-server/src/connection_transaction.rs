use garnet_common::ArgSlice;
use garnet_common::RespParseError;
use garnet_common::parse_resp_command_arg_slices_dynamic;
use std::sync::Arc;

use crate::RequestProcessor;
use crate::ShardOwnerThreadPool;
use crate::connection_routing::owner_shard_for_command;
use crate::dispatch_from_arg_slices;

#[derive(Default)]
pub(crate) struct ConnectionTransactionState {
    pub(crate) in_multi: bool,
    pub(crate) queued_frames: Vec<Vec<u8>>,
    pub(crate) watched_keys: Vec<(Vec<u8>, u64)>,
    pub(crate) transaction_slot: Option<u16>,
    pub(crate) aborted: bool,
}

impl ConnectionTransactionState {
    pub(crate) fn reset(&mut self) {
        self.in_multi = false;
        self.queued_frames.clear();
        self.watched_keys.clear();
        self.transaction_slot = None;
        self.aborted = false;
    }

    pub(crate) fn clear_watches(&mut self) {
        self.watched_keys.clear();
    }

    pub(crate) fn watch_key(&mut self, key: &[u8], version: u64) {
        if let Some((_, watched_version)) = self
            .watched_keys
            .iter_mut()
            .find(|(watched_key, _)| watched_key.as_slice() == key)
        {
            *watched_version = version;
            return;
        }
        self.watched_keys.push((key.to_vec(), version));
    }

    pub(crate) fn set_transaction_slot_or_abort(&mut self, slot: u16) -> bool {
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

pub(crate) fn execute_transaction_queue(
    processor: &Arc<RequestProcessor>,
    owner_thread_pool: &Arc<ShardOwnerThreadPool>,
    transaction: &mut ConnectionTransactionState,
    responses: &mut Vec<u8>,
    max_resp_arguments: usize,
) {
    let queued = std::mem::take(&mut transaction.queued_frames);
    transaction.in_multi = false;
    transaction.watched_keys.clear();
    transaction.transaction_slot = None;
    transaction.aborted = false;

    let owner_shard = transaction_owner_shard(processor, &queued, max_resp_arguments).unwrap_or(0);
    let queued_len = queued.len();
    let routed_processor = Arc::clone(processor);
    let item_responses = owner_thread_pool
        .execute_sync(owner_shard, move || {
            execute_transaction_queue_on_owner_thread(&routed_processor, queued, max_resp_arguments)
        })
        .unwrap_or_else(|_| vec![b"-ERR owner routing execution failed\r\n".to_vec(); queued_len]);

    responses.push(b'*');
    responses.extend_from_slice(item_responses.len().to_string().as_bytes());
    responses.extend_from_slice(b"\r\n");
    for item_response in item_responses {
        responses.extend_from_slice(&item_response);
    }
}

fn transaction_owner_shard(
    processor: &Arc<RequestProcessor>,
    queued: &[Vec<u8>],
    max_resp_arguments: usize,
) -> Option<usize> {
    let frame = queued.first()?;
    let mut args = vec![ArgSlice::EMPTY; 64.min(max_resp_arguments.max(1))];
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
}

fn execute_transaction_queue_on_owner_thread(
    processor: &RequestProcessor,
    queued: Vec<Vec<u8>>,
    max_resp_arguments: usize,
) -> Vec<Vec<u8>> {
    let mut args = vec![ArgSlice::EMPTY; 64.min(max_resp_arguments.max(1))];
    let mut responses = Vec::with_capacity(queued.len());
    for frame in queued {
        let mut item_response = Vec::new();
        match parse_resp_command_arg_slices_dynamic(&frame, &mut args, max_resp_arguments) {
            Ok(meta) if meta.bytes_consumed == frame.len() => {
                // SAFETY: parsed ArgSlice values reference bytes in `frame` for this scope.
                let _command = unsafe { dispatch_from_arg_slices(&args[..meta.argument_count]) };
                match processor.execute(&args[..meta.argument_count], &mut item_response) {
                    Ok(()) => {}
                    Err(error) => error.append_resp_error(&mut item_response),
                }
            }
            Err(RespParseError::ArgumentCapacityExceeded { .. }) => {
                item_response.extend_from_slice(b"-ERR too many arguments in request\r\n");
            }
            _ => item_response.extend_from_slice(b"-ERR protocol error\r\n"),
        }
        responses.push(item_response);
    }
    responses
}
