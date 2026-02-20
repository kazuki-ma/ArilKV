use super::*;

impl RequestProcessor {
    pub(super) fn handle_sadd(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "SADD",
                expected: "SADD key member [member ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut set = self.load_set_object(&key)?.unwrap_or_default();
        let mut inserted = 0i64;
        for member in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            if set.insert(unsafe { member.as_slice() }.to_vec()) {
                inserted += 1;
            }
        }
        self.save_set_object(&key, &set)?;
        append_integer(response_out, inserted);
        Ok(())
    }

    pub(super) fn handle_srem(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "SREM",
                expected: "SREM key member [member ...]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let mut set = match self.load_set_object(&key)? {
            Some(set) => set,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        let mut removed = 0i64;
        for member in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            if set.remove(unsafe { member.as_slice() }) {
                removed += 1;
            }
        }

        if set.is_empty() {
            let _ = self.object_delete(&key)?;
        } else {
            self.save_set_object(&key, &set)?;
        }
        append_integer(response_out, removed);
        Ok(())
    }

    pub(super) fn handle_smembers(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "SMEMBERS",
                expected: "SMEMBERS key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let set = match self.load_set_object(&key)? {
            Some(set) => set,
            None => {
                response_out.extend_from_slice(b"*0\r\n");
                return Ok(());
            }
        };

        response_out.push(b'*');
        response_out.extend_from_slice(set.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for member in &set {
            append_bulk_string(response_out, member);
        }
        Ok(())
    }

    pub(super) fn handle_sismember(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "SISMEMBER",
                expected: "SISMEMBER key member",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let member = unsafe { args[2].as_slice() };
        let set = match self.load_set_object(&key)? {
            Some(set) => set,
            None => {
                append_integer(response_out, 0);
                return Ok(());
            }
        };
        append_integer(response_out, if set.contains(member) { 1 } else { 0 });
        Ok(())
    }
}
