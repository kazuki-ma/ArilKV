use super::*;
use crate::command_spec::command_names_for_command_response;

impl RequestProcessor {
    pub(super) fn handle_ping(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        match args.len() {
            1 => {
                append_simple_string(response_out, b"PONG");
                Ok(())
            }
            2 => {
                // SAFETY: caller guarantees argument backing memory validity.
                append_bulk_string(response_out, unsafe { args[1].as_slice() });
                Ok(())
            }
            _ => Err(RequestExecutionError::WrongArity {
                command: "PING",
                expected: "PING [message]",
            }),
        }
    }

    pub(super) fn handle_echo(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "ECHO",
                expected: "ECHO message",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        append_bulk_string(response_out, unsafe { args[1].as_slice() });
        Ok(())
    }

    pub(super) fn handle_info(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "INFO",
                expected: "INFO",
            });
        }

        let dbsize = self.active_key_count()?;
        let payload = format!(
            "# Server\r\nredis_version:garnet-rs\r\n# Stats\r\ndbsize:{}\r\n",
            dbsize
        );
        append_bulk_string(response_out, payload.as_bytes());
        Ok(())
    }

    pub(super) fn handle_dbsize(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "DBSIZE",
                expected: "DBSIZE",
            });
        }
        append_integer(response_out, self.active_key_count()?);
        Ok(())
    }

    pub(super) fn handle_command(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "COMMAND",
                expected: "COMMAND",
            });
        }
        append_bulk_array(response_out, command_names_for_command_response());
        Ok(())
    }

    fn active_key_count(&self) -> Result<i64, RequestExecutionError> {
        let mut keys: HashSet<Vec<u8>> = self.string_keys_snapshot().into_iter().collect();
        keys.extend(self.object_keys_snapshot());

        let mut count = 0i64;
        for key in keys {
            self.expire_key_if_needed(&key)?;
            let string_exists = self.key_exists(&key)?;
            let object_exists = self.object_key_exists(&key)?;
            if string_exists || object_exists {
                count += 1;
            }
            if !string_exists {
                self.untrack_string_key(&key);
            }
            if !object_exists {
                self.untrack_object_key(&key);
            }
        }
        Ok(count)
    }
}
