use super::*;

impl RequestProcessor {
    pub(super) fn handle_lpush(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "LPUSH",
                expected: "LPUSH key value [value ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut list = self.load_list_object(&key)?.unwrap_or_default();
        for value in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            list.insert(0, unsafe { value.as_slice() }.to_vec());
        }
        self.save_list_object(&key, &list)?;
        append_integer(response_out, list.len() as i64);
        Ok(())
    }

    pub(super) fn handle_rpush(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "RPUSH",
                expected: "RPUSH key value [value ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut list = self.load_list_object(&key)?.unwrap_or_default();
        for value in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            list.push(unsafe { value.as_slice() }.to_vec());
        }
        self.save_list_object(&key, &list)?;
        append_integer(response_out, list.len() as i64);
        Ok(())
    }

    pub(super) fn handle_lpop(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "LPOP",
                expected: "LPOP key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut list = match self.load_list_object(&key)? {
            Some(list) => list,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };
        if list.is_empty() {
            let _ = self.object_delete(&key)?;
            append_null_bulk_string(response_out);
            return Ok(());
        }

        let value = list.remove(0);
        if list.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_list_object(&key, &list)?;
        }
        append_bulk_string(response_out, &value);
        Ok(())
    }

    pub(super) fn handle_rpop(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "RPOP",
                expected: "RPOP key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut list = match self.load_list_object(&key)? {
            Some(list) => list,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };
        let value = match list.pop() {
            Some(value) => value,
            None => {
                let _ = self.object_delete(&key)?;
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };

        if list.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_list_object(&key, &list)?;
        }
        append_bulk_string(response_out, &value);
        Ok(())
    }

    pub(super) fn handle_lrange(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "LRANGE",
                expected: "LRANGE key start stop",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let start = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let stop = parse_i64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        let list = match self.load_list_object(&key)? {
            Some(list) => list,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        let len = list.len() as i64;
        if len == 0 {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }

        let mut normalized_start = if start < 0 { len + start } else { start };
        let mut normalized_stop = if stop < 0 { len + stop } else { stop };
        if normalized_start < 0 {
            normalized_start = 0;
        }
        if normalized_stop < 0 || normalized_start >= len {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }
        if normalized_stop >= len {
            normalized_stop = len - 1;
        }
        if normalized_start > normalized_stop {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }

        let count = (normalized_stop - normalized_start + 1) as usize;
        response_out.push(b'*');
        response_out.extend_from_slice(count.to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for index in normalized_start..=normalized_stop {
            append_bulk_string(response_out, &list[index as usize]);
        }
        Ok(())
    }
}
