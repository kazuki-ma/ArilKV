use garnet_common::ArgSlice;
use std::sync::Arc;

use crate::connection_routing::owner_shard_for_command;
use crate::{CommandId, RequestExecutionError, RequestProcessor, ShardOwnerThreadPool};

const MAX_ROUTED_ARGUMENTS: usize = 64;

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

#[derive(Clone, Debug)]
pub(crate) struct OwnedFrameArgs {
    frame: Vec<u8>,
    arg_offsets_and_lengths: Vec<(usize, usize)>,
}

pub(crate) fn capture_owned_frame_args(
    frame: &[u8],
    args: &[ArgSlice],
) -> Result<OwnedFrameArgs, RoutedExecutionError> {
    if args.is_empty() || args.len() > MAX_ROUTED_ARGUMENTS {
        return Err(RoutedExecutionError::Protocol);
    }

    let frame_start = frame.as_ptr() as usize;
    let frame_end = frame_start + frame.len();
    let mut arg_offsets_and_lengths = Vec::with_capacity(args.len());
    for arg in args {
        let arg_ptr = arg.as_ptr() as usize;
        let arg_len = arg.len();
        let arg_end = arg_ptr
            .checked_add(arg_len)
            .ok_or(RoutedExecutionError::Protocol)?;
        if arg_ptr < frame_start || arg_ptr > frame_end || arg_end > frame_end {
            return Err(RoutedExecutionError::Protocol);
        }
        arg_offsets_and_lengths.push((arg_ptr - frame_start, arg_len));
    }

    Ok(OwnedFrameArgs {
        frame: frame.to_vec(),
        arg_offsets_and_lengths,
    })
}

pub(crate) fn execute_owned_frame_args_via_processor(
    processor: &RequestProcessor,
    owned_args: &OwnedFrameArgs,
) -> Result<Vec<u8>, RoutedExecutionError> {
    if owned_args.arg_offsets_and_lengths.is_empty()
        || owned_args.arg_offsets_and_lengths.len() > MAX_ROUTED_ARGUMENTS
    {
        return Err(RoutedExecutionError::Protocol);
    }

    let mut args = [ArgSlice::EMPTY; MAX_ROUTED_ARGUMENTS];
    for (index, (offset, len)) in owned_args
        .arg_offsets_and_lengths
        .iter()
        .copied()
        .enumerate()
    {
        let end = offset
            .checked_add(len)
            .ok_or(RoutedExecutionError::Protocol)?;
        let slice = owned_args
            .frame
            .get(offset..end)
            .ok_or(RoutedExecutionError::Protocol)?;
        args[index] = ArgSlice::from_slice(slice).map_err(|_| RoutedExecutionError::Protocol)?;
    }

    let mut response = Vec::new();
    processor
        .execute(
            &args[..owned_args.arg_offsets_and_lengths.len()],
            &mut response,
        )
        .map_err(RoutedExecutionError::Request)?;
    Ok(response)
}

pub(crate) fn execute_frame_on_owner_thread(
    processor: &Arc<RequestProcessor>,
    owner_thread_pool: &Arc<ShardOwnerThreadPool>,
    args: &[ArgSlice],
    command: CommandId,
    frame: &[u8],
) -> Result<Vec<u8>, OwnerThreadExecutionError> {
    let shard_index = owner_shard_for_command(processor, args, command);
    let owned_args = capture_owned_frame_args(frame, args).map_err(|error| match error {
        RoutedExecutionError::Protocol => OwnerThreadExecutionError::Protocol,
        RoutedExecutionError::Request(request_error) => {
            OwnerThreadExecutionError::Request(request_error)
        }
    })?;
    let routed_processor = Arc::clone(processor);
    owner_thread_pool
        .execute_sync(shard_index, move || {
            execute_owned_frame_args_via_processor(&routed_processor, &owned_args)
        })
        .map_err(|_| OwnerThreadExecutionError::OwnerThreadUnavailable)?
        .map_err(|error| match error {
            RoutedExecutionError::Protocol => OwnerThreadExecutionError::Protocol,
            RoutedExecutionError::Request(request_error) => {
                OwnerThreadExecutionError::Request(request_error)
            }
        })
}

#[cfg(test)]
pub(crate) fn execute_owned_args_via_processor(
    processor: &RequestProcessor,
    owned_args: &[Vec<u8>],
) -> Result<Vec<u8>, RoutedExecutionError> {
    if owned_args.is_empty() || owned_args.len() > MAX_ROUTED_ARGUMENTS {
        return Err(RoutedExecutionError::Protocol);
    }

    let mut args = [ArgSlice::EMPTY; MAX_ROUTED_ARGUMENTS];
    for (index, arg) in owned_args.iter().enumerate() {
        args[index] = ArgSlice::from_slice(arg).map_err(|_| RoutedExecutionError::Protocol)?;
    }

    let mut response = Vec::new();
    processor
        .execute(&args[..owned_args.len()], &mut response)
        .map_err(RoutedExecutionError::Request)?;
    Ok(response)
}

#[cfg(test)]
pub(crate) fn execute_frame_via_processor(
    processor: &RequestProcessor,
    frame: &[u8],
) -> Result<Vec<u8>, RoutedExecutionError> {
    let mut args = [ArgSlice::EMPTY; MAX_ROUTED_ARGUMENTS];
    let meta = garnet_common::parse_resp_command_arg_slices(frame, &mut args)
        .map_err(|_| RoutedExecutionError::Protocol)?;
    if meta.bytes_consumed != frame.len() {
        return Err(RoutedExecutionError::Protocol);
    }
    let mut response = Vec::new();
    processor
        .execute(&args[..meta.argument_count], &mut response)
        .map_err(RoutedExecutionError::Request)?;
    Ok(response)
}
