use garnet_common::ArgSlice;
#[cfg(test)]
use garnet_common::parse_resp_command_arg_slices_dynamic;
use std::sync::Arc;

use crate::ClientId;
use crate::CommandId;
use crate::RequestExecutionError;
use crate::RequestProcessor;
use crate::ShardOwnerThreadPool;
use crate::connection_routing::owner_shard_for_command;
#[cfg(test)]
const TEST_MAX_ROUTED_ARGUMENTS: usize = 1_048_576;

#[derive(Debug)]
pub(crate) enum RoutedExecutionError {
    Protocol,
    Request(RequestExecutionError),
}

#[derive(Debug)]
pub(crate) enum OwnerThreadExecutionError {
    Protocol,
    Request(RequestExecutionError),
    OwnerThreadUnavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ArgSpan {
    offset: usize,
    length: usize,
}

impl ArgSpan {
    const fn new(offset: usize, length: usize) -> Self {
        Self { offset, length }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct OwnedFrameArgs {
    frame: Vec<u8>,
    arg_spans: Vec<ArgSpan>,
}

#[inline]
fn map_routed_owner_error(error: RoutedExecutionError) -> OwnerThreadExecutionError {
    match error {
        RoutedExecutionError::Protocol => OwnerThreadExecutionError::Protocol,
        RoutedExecutionError::Request(request_error) => {
            OwnerThreadExecutionError::Request(request_error)
        }
    }
}

fn parse_owned_frame_args(
    owned_args: &OwnedFrameArgs,
) -> Result<Vec<ArgSlice>, RoutedExecutionError> {
    if owned_args.arg_spans.is_empty() {
        return Err(RoutedExecutionError::Protocol);
    }

    let mut args = vec![ArgSlice::EMPTY; owned_args.arg_spans.len()];
    for (index, span) in owned_args.arg_spans.iter().copied().enumerate() {
        let end = span
            .offset
            .checked_add(span.length)
            .ok_or(RoutedExecutionError::Protocol)?;
        let slice = owned_args
            .frame
            .get(span.offset..end)
            .ok_or(RoutedExecutionError::Protocol)?;
        args[index] = ArgSlice::from_slice(slice).map_err(|_| RoutedExecutionError::Protocol)?;
    }

    Ok(args)
}

pub(crate) fn capture_owned_frame_args(
    frame: &[u8],
    args: &[ArgSlice],
) -> Result<OwnedFrameArgs, RoutedExecutionError> {
    if args.is_empty() {
        return Err(RoutedExecutionError::Protocol);
    }

    let frame_start = frame.as_ptr() as usize;
    let frame_end = frame_start + frame.len();
    let mut arg_spans = Vec::with_capacity(args.len());
    for arg in args {
        let arg_ptr = arg.as_ptr() as usize;
        let arg_len = arg.len();
        let arg_end = arg_ptr
            .checked_add(arg_len)
            .ok_or(RoutedExecutionError::Protocol)?;
        if arg_ptr < frame_start || arg_ptr > frame_end || arg_end > frame_end {
            return Err(RoutedExecutionError::Protocol);
        }
        arg_spans.push(ArgSpan::new(arg_ptr - frame_start, arg_len));
    }

    Ok(OwnedFrameArgs {
        frame: frame.to_vec(),
        arg_spans,
    })
}

pub(crate) fn execute_owned_frame_args_via_processor(
    processor: &RequestProcessor,
    owned_args: &OwnedFrameArgs,
    client_no_touch: bool,
    client_id: Option<ClientId>,
) -> Result<Vec<u8>, RoutedExecutionError> {
    let args = parse_owned_frame_args(owned_args)?;

    let mut response = Vec::new();
    processor
        .execute_with_client_context(&args, &mut response, client_no_touch, client_id)
        .map_err(RoutedExecutionError::Request)?;
    Ok(response)
}

pub(crate) fn execute_frame_on_owner_thread(
    processor: &Arc<RequestProcessor>,
    owner_thread_pool: &Arc<ShardOwnerThreadPool>,
    args: &[ArgSlice],
    command: CommandId,
    frame: &[u8],
    client_no_touch: bool,
    client_id: Option<ClientId>,
) -> Result<Vec<u8>, OwnerThreadExecutionError> {
    if owner_thread_pool.is_inline_execution() {
        let mut response = Vec::new();
        processor
            .execute_with_client_context(args, &mut response, client_no_touch, client_id)
            .map_err(OwnerThreadExecutionError::Request)?;
        return Ok(response);
    }

    let shard_index = owner_shard_for_command(processor, args, command);
    let owned_args = capture_owned_frame_args(frame, args).map_err(map_routed_owner_error)?;
    let routed_processor = Arc::clone(processor);
    owner_thread_pool
        .execute_sync(shard_index, move || {
            execute_owned_frame_args_via_processor(
                &routed_processor,
                &owned_args,
                client_no_touch,
                client_id,
            )
        })
        .map_err(|_| OwnerThreadExecutionError::OwnerThreadUnavailable)?
        .map_err(map_routed_owner_error)
}

#[cfg(test)]
pub(crate) fn execute_owned_args_via_processor(
    processor: &RequestProcessor,
    owned_args: &[Vec<u8>],
) -> Result<Vec<u8>, RoutedExecutionError> {
    if owned_args.is_empty() {
        return Err(RoutedExecutionError::Protocol);
    }

    let mut args = vec![ArgSlice::EMPTY; owned_args.len()];
    for (index, arg) in owned_args.iter().enumerate() {
        args[index] = ArgSlice::from_slice(arg).map_err(|_| RoutedExecutionError::Protocol)?;
    }

    let mut response = Vec::new();
    processor
        .execute_with_client_no_touch(&args, &mut response, false)
        .map_err(RoutedExecutionError::Request)?;
    Ok(response)
}

#[cfg(test)]
pub(crate) fn execute_frame_via_processor(
    processor: &RequestProcessor,
    frame: &[u8],
) -> Result<Vec<u8>, RoutedExecutionError> {
    let mut args = vec![ArgSlice::EMPTY; 64];
    let meta = parse_resp_command_arg_slices_dynamic(frame, &mut args, TEST_MAX_ROUTED_ARGUMENTS)
        .map_err(|_| RoutedExecutionError::Protocol)?;
    if meta.bytes_consumed != frame.len() {
        return Err(RoutedExecutionError::Protocol);
    }
    let mut response = Vec::new();
    processor
        .execute_with_client_no_touch(&args[..meta.argument_count], &mut response, false)
        .map_err(RoutedExecutionError::Request)?;
    Ok(response)
}
