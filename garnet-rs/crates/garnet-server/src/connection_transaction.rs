use garnet_common::{parse_resp_command_arg_slices, ArgSlice};
use std::sync::Arc;

use crate::connection_owner_routing::{execute_frame_on_owner_thread, OwnerThreadExecutionError};
use crate::dispatch_from_arg_slices;
use crate::{RequestProcessor, ShardOwnerThreadPool};

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
) {
    let queued = std::mem::take(&mut transaction.queued_frames);
    transaction.in_multi = false;
    transaction.watched_keys.clear();
    transaction.transaction_slot = None;
    transaction.aborted = false;

    responses.push(b'*');
    responses.extend_from_slice(queued.len().to_string().as_bytes());
    responses.extend_from_slice(b"\r\n");

    let mut args = [ArgSlice::EMPTY; 64];
    for frame in queued {
        let mut item_response = Vec::new();
        match parse_resp_command_arg_slices(&frame, &mut args) {
            Ok(meta) if meta.bytes_consumed == frame.len() => {
                // SAFETY: parsed ArgSlice values reference bytes in `frame` for this scope.
                let command = unsafe { dispatch_from_arg_slices(&args[..meta.argument_count]) };
                match execute_frame_on_owner_thread(
                    processor,
                    owner_thread_pool,
                    &args[..meta.argument_count],
                    command,
                    &frame,
                ) {
                    Ok(response) => item_response.extend_from_slice(&response),
                    Err(OwnerThreadExecutionError::Request(error)) => {
                        error.append_resp_error(&mut item_response);
                    }
                    Err(OwnerThreadExecutionError::Protocol) => {
                        item_response.extend_from_slice(b"-ERR protocol error\r\n");
                    }
                    Err(OwnerThreadExecutionError::OwnerThreadUnavailable) => {
                        item_response.extend_from_slice(b"-ERR owner routing execution failed\r\n");
                    }
                }
            }
            _ => item_response.extend_from_slice(b"-ERR protocol error\r\n"),
        }
        responses.extend_from_slice(&item_response);
    }
}
