use super::*;

impl RequestProcessor {
    pub(super) fn handle_hset(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 4 || ((args.len() - 2) % 2 != 0) {
            return Err(RequestExecutionError::WrongArity {
                command: "HSET",
                expected: "HSET key field value [field value ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut hash = self.load_hash_object(&key)?.unwrap_or_default();
        let mut inserted = 0i64;

        let mut index = 2usize;
        while index < args.len() {
            // SAFETY: caller guarantees argument backing memory validity.
            let field = unsafe { args[index].as_slice() }.to_vec();
            // SAFETY: caller guarantees argument backing memory validity.
            let value = unsafe { args[index + 1].as_slice() }.to_vec();
            if hash.insert(field, value).is_none() {
                inserted += 1;
            }
            index += 2;
        }

        self.save_hash_object(&key, &hash)?;
        append_integer(response_out, inserted);
        Ok(())
    }

    pub(super) fn handle_hget(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "HGET",
                expected: "HGET key field",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let field = unsafe { args[2].as_slice() };
        let hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };

        match hash.get(field) {
            Some(value) => append_bulk_string(response_out, value),
            None => append_null_bulk_string(response_out),
        }
        Ok(())
    }

    pub(super) fn handle_hdel(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "HDEL",
                expected: "HDEL key field [field ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };

        let mut removed = 0i64;
        for field in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            if hash.remove(unsafe { field.as_slice() }).is_some() {
                removed += 1;
            }
        }

        if hash.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_hash_object(&key, &hash)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_hgetall(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "HGETALL",
                expected: "HGETALL key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let hash = match self.load_hash_object(&key)? {
            Some(hash) => hash,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        let pair_count = hash.len().saturating_mul(2);
        response_out.push(b'*');
        response_out.extend_from_slice(pair_count.to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for (field, value) in &hash {
            append_bulk_string(response_out, field);
            append_bulk_string(response_out, value);
        }
        Ok(())
    }
}
