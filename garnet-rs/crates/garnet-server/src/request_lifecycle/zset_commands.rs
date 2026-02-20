use super::*;

impl RequestProcessor {
    pub(super) fn handle_zadd(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 4 || ((args.len() - 2) % 2 != 0) {
            return Err(RequestExecutionError::WrongArity {
                command: "ZADD",
                expected: "ZADD key score member [score member ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut zset = self.load_zset_object(&key)?.unwrap_or_default();
        let mut inserted = 0i64;

        let mut index = 2usize;
        while index < args.len() {
            // SAFETY: caller guarantees argument backing memory validity.
            let score = parse_f64_ascii(unsafe { args[index].as_slice() })
                .ok_or(RequestExecutionError::ValueNotFloat)?;
            // SAFETY: caller guarantees argument backing memory validity.
            let member = unsafe { args[index + 1].as_slice() }.to_vec();
            if zset.insert(member, score).is_none() {
                inserted += 1;
            }
            index += 2;
        }

        self.save_zset_object(&key, &zset)?;
        append_integer(response_out, inserted);
        Ok(())
    }

    pub(super) fn handle_zrem(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "ZREM",
                expected: "ZREM key member [member ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let mut removed = 0i64;
        for member in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            if zset.remove(unsafe { member.as_slice() }).is_some() {
                removed += 1;
            }
        }

        if zset.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_zset_object(&key, &zset)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_zrange(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 4 {
            return Err(RequestExecutionError::WrongArity {
                command: "ZRANGE",
                expected: "ZRANGE key start stop",
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
        let zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        let mut entries: Vec<(&Vec<u8>, f64)> = zset
            .iter()
            .map(|(member, score)| (member, *score))
            .collect();
        entries.sort_by(|(lhs_member, lhs_score), (rhs_member, rhs_score)| {
            lhs_score
                .partial_cmp(rhs_score)
                .expect("zset scores are finite")
                .then_with(|| lhs_member.cmp(rhs_member))
        });

        let len = entries.len() as i64;
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
            append_bulk_string(response_out, entries[index as usize].0);
        }
        Ok(())
    }

    pub(super) fn handle_zscore(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "ZSCORE",
                expected: "ZSCORE key member",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let member = unsafe { args[2].as_slice() };
        let zset = match self.load_zset_object(&key)? {
            Some(zset) => zset,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };

        match zset.get(member) {
            Some(score) => append_bulk_string(response_out, score.to_string().as_bytes()),
            None => append_null_bulk_string(response_out),
        }
        Ok(())
    }
}
