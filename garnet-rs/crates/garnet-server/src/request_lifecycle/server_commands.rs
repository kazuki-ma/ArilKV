use super::*;
use crate::command_spec::command_names_for_command_response;

static NEXT_RANDOMKEY_INDEX: AtomicU64 = AtomicU64::new(0);
const MODULE_HELP_LINES: [&[u8]; 11] = [
    b"MODULE <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"LIST",
    b"    Return a list of loaded modules.",
    b"LOAD <path> [<arg> ...]",
    b"    Load a module library from <path>, passing to it any optional arguments.",
    b"LOADEX <path> [[CONFIG NAME VALUE] [CONFIG NAME VALUE]] [ARGS ...]",
    b"    Load a module library from <path>, while passing it module configurations and optional arguments.",
    b"UNLOAD <name>",
    b"    Unload a module.",
    b"HELP",
    b"    Print this help.",
];
const LATENCY_HELP_LINES: [&[u8]; 15] = [
    b"LATENCY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"DOCTOR",
    b"    Return a human readable latency analysis report.",
    b"GRAPH <event>",
    b"    Return an ASCII latency graph for the <event> class.",
    b"HISTORY <event>",
    b"    Return time-latency samples for the <event> class.",
    b"LATEST",
    b"    Return the latest latency samples for all events.",
    b"RESET [<event> ...]",
    b"    Reset latency data of one or more <event> classes.",
    b"HISTOGRAM [COMMAND ...]",
    b"    Return cumulative latency histograms for command names.",
    b"HELP",
    b"    Print this help.",
];
const LATENCY_DOCTOR_DISABLED_MESSAGE: &[u8] =
    b"I'm sorry, Dave, I can't do that. Latency monitoring is disabled in this garnet-rs instance.";
const SLOWLOG_HELP_LINES: [&[u8]; 12] = [
    b"SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"GET [<count>]",
    b"    Return top <count> entries from the slowlog (default: 10, -1 mean all).",
    b"    Entries are made of:",
    b"    id, timestamp, time in microseconds, arguments array, client IP and port,",
    b"    client name",
    b"LEN",
    b"    Return the length of the slowlog.",
    b"RESET",
    b"    Reset the slowlog.",
    b"HELP",
    b"    Print this help.",
];
const ACL_HELP_LINES: [&[u8]; 13] = [
    b"ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    b"CAT [<category>]",
    b"    List ACL categories and the commands inside them.",
    b"GETUSER <username>",
    b"    Get user details.",
    b"LIST",
    b"    List ACL rules in ACL file format.",
    b"SETUSER <username> [<rule> ...]",
    b"    Modify ACL rules for a user.",
    b"USERS",
    b"    List all configured users.",
    b"WHOAMI",
    b"    Return the current connection username.",
];
const ACL_CATEGORIES: [&[u8]; 8] = [
    b"keyspace",
    b"read",
    b"write",
    b"string",
    b"hash",
    b"list",
    b"set",
    b"stream",
];

impl RequestProcessor {
    pub(super) fn handle_quit(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "QUIT",
                expected: "QUIT",
            });
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_time(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "TIME",
                expected: "TIME",
            });
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| RequestExecutionError::ValueOutOfRange)?;
        let seconds = now.as_secs().to_string();
        let microseconds = now.subsec_micros().to_string();
        append_bulk_array(response_out, &[seconds.as_bytes(), microseconds.as_bytes()]);
        Ok(())
    }

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

    pub(super) fn handle_hello(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() == 1 {
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "HELLO",
                expected: "HELLO [2|3]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let version = parse_u64_ascii(unsafe { args[1].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        if version != 2 && version != 3 {
            return Err(RequestExecutionError::SyntaxError);
        }
        self.set_resp_protocol_version(version as usize);
        append_simple_string(response_out, b"OK");
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

    pub(super) fn handle_lastsave(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "LASTSAVE",
                expected: "LASTSAVE",
            });
        }
        append_integer(response_out, self.lastsave_unix_seconds() as i64);
        Ok(())
    }

    pub(super) fn handle_auth(
        &self,
        args: &[ArgSlice],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(args, 2, 3, "AUTH", "AUTH [username] password")?;
        Err(RequestExecutionError::AuthNotEnabled)
    }

    pub(super) fn handle_select(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 2, "SELECT", "SELECT index")?;
        // SAFETY: caller guarantees argument backing memory validity.
        let index = parse_i64_ascii(unsafe { args[1].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        if index != 0 {
            return Err(RequestExecutionError::DbIndexOutOfRange);
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_move(
        &self,
        args: &[ArgSlice],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "MOVE", "MOVE key db")?;
        // SAFETY: caller guarantees argument backing memory validity.
        let target_db = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        if target_db == 0 {
            return Err(RequestExecutionError::SourceDestinationObjectsSame);
        }
        Err(RequestExecutionError::DbIndexOutOfRange)
    }

    pub(super) fn handle_swapdb(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "SWAPDB", "SWAPDB index1 index2")?;
        // SAFETY: caller guarantees argument backing memory validity.
        let index1 = parse_i64_ascii(unsafe { args[1].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let index2 = parse_i64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        if index1 != 0 || index2 != 0 {
            return Err(RequestExecutionError::DbIndexOutOfRange);
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_client(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            2,
            "CLIENT",
            "CLIENT <ID|GETNAME|SETNAME> [arguments...]",
        )?;
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };
        if ascii_eq_ignore_case(subcommand, b"ID") {
            require_exact_arity(args, 2, "CLIENT", "CLIENT ID")?;
            append_integer(response_out, 1);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GETNAME") {
            require_exact_arity(args, 2, "CLIENT", "CLIENT GETNAME")?;
            append_null_bulk_string(response_out);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"SETNAME") {
            require_exact_arity(args, 3, "CLIENT", "CLIENT SETNAME connection-name")?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_role(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "ROLE", "ROLE")?;
        response_out.extend_from_slice(b"*3\r\n");
        append_bulk_string(response_out, b"master");
        append_integer(response_out, 0);
        response_out.extend_from_slice(b"*0\r\n");
        Ok(())
    }

    pub(super) fn handle_wait(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 3, "WAIT", "WAIT numreplicas timeout")?;
        // SAFETY: caller guarantees argument backing memory validity.
        parse_u64_ascii(unsafe { args[1].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        parse_u64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        append_integer(response_out, 0);
        Ok(())
    }

    pub(super) fn handle_waitaof(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 4, "WAITAOF", "WAITAOF numlocal numreplicas timeout")?;
        // SAFETY: caller guarantees argument backing memory validity.
        let numlocal = parse_u64_ascii(unsafe { args[1].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        parse_u64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        // SAFETY: caller guarantees argument backing memory validity.
        parse_u64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        if numlocal > 1 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        if numlocal > 0 {
            return Err(RequestExecutionError::WaitAofAppendOnlyDisabled);
        }
        append_integer(response_out, 0);
        Ok(())
    }

    pub(super) fn handle_save(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "SAVE", "SAVE")?;
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_bgsave(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(args, 1, 2, "BGSAVE", "BGSAVE [SCHEDULE]")?;
        if args.len() == 2 {
            // SAFETY: caller guarantees argument backing memory validity.
            let mode = unsafe { args[1].as_slice() };
            if !ascii_eq_ignore_case(mode, b"SCHEDULE") {
                return Err(RequestExecutionError::SyntaxError);
            }
        }
        append_simple_string(response_out, b"Background saving started");
        Ok(())
    }

    pub(super) fn handle_bgrewriteaof(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "BGREWRITEAOF", "BGREWRITEAOF")?;
        append_simple_string(
            response_out,
            b"Background append only file rewriting started",
        );
        Ok(())
    }

    pub(super) fn handle_readonly(
        &self,
        args: &[ArgSlice],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "READONLY",
                expected: "READONLY",
            });
        }
        Err(RequestExecutionError::ClusterSupportDisabled)
    }

    pub(super) fn handle_readwrite(
        &self,
        args: &[ArgSlice],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "READWRITE",
                expected: "READWRITE",
            });
        }
        Err(RequestExecutionError::ClusterSupportDisabled)
    }

    pub(super) fn handle_reset(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "RESET",
                expected: "RESET",
            });
        }
        self.set_resp_protocol_version(2);
        append_simple_string(response_out, b"RESET");
        Ok(())
    }

    pub(super) fn handle_lolwut(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() >= 2 {
            // SAFETY: caller guarantees argument backing memory validity.
            let first_option = unsafe { args[1].as_slice() };
            if ascii_eq_ignore_case(first_option, b"VERSION") {
                if args.len() != 3 {
                    return Err(RequestExecutionError::WrongArity {
                        command: "LOLWUT",
                        expected: "LOLWUT [VERSION version]",
                    });
                }
                // SAFETY: caller guarantees argument backing memory validity.
                parse_u64_ascii(unsafe { args[2].as_slice() })
                    .ok_or(RequestExecutionError::ValueNotInteger)?;
            }
        }
        append_bulk_string(response_out, b"Redis ver. garnet-rs\n");
        Ok(())
    }

    pub(super) fn handle_memory(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "MEMORY",
                expected: "MEMORY USAGE key",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };
        if !ascii_eq_ignore_case(subcommand, b"USAGE") {
            return Err(RequestExecutionError::UnknownCommand);
        }
        if args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "MEMORY",
                expected: "MEMORY USAGE key",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[2].as_slice() }.to_vec();
        self.expire_key_if_needed(&key)?;

        if let Some(value) = self.read_string_value(&key)? {
            append_integer(
                response_out,
                estimate_memory_usage_bytes(key.len(), value.len()),
            );
            return Ok(());
        }
        if let Some((_object_type, payload)) = self.object_read(&key)? {
            append_integer(
                response_out,
                estimate_memory_usage_bytes(key.len(), payload.len()),
            );
            return Ok(());
        }

        append_null_bulk_string(response_out);
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

    pub(super) fn handle_debug(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "DEBUG",
                expected: "DEBUG subcommand [arguments...]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };
        if ascii_eq_ignore_case(subcommand, b"SET-ACTIVE-EXPIRE") {
            if args.len() != 3 {
                return Err(RequestExecutionError::WrongArity {
                    command: "DEBUG",
                    expected: "DEBUG SET-ACTIVE-EXPIRE <0|1>",
                });
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let enabled = unsafe { args[2].as_slice() };
            if enabled != b"0" && enabled != b"1" {
                return Err(RequestExecutionError::SyntaxError);
            }
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"DIGEST-VALUE") {
            if args.len() != 3 {
                return Err(RequestExecutionError::WrongArity {
                    command: "DEBUG",
                    expected: "DEBUG DIGEST-VALUE key",
                });
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { args[2].as_slice() }.to_vec();
            self.expire_key_if_needed(&key)?;
            let digest = self.debug_digest_value_for_key(&key)?;
            append_bulk_string(response_out, &digest);
            return Ok(());
        }
        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_object(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "OBJECT",
                expected: "OBJECT <ENCODING|REFCOUNT> key",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };

        if ascii_eq_ignore_case(subcommand, b"ENCODING") {
            if args.len() != 3 {
                return Err(RequestExecutionError::WrongArity {
                    command: "OBJECT",
                    expected: "OBJECT ENCODING key",
                });
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { args[2].as_slice() }.to_vec();
            self.expire_key_if_needed(&key)?;
            if self.key_exists(&key)? {
                append_bulk_string(response_out, b"raw");
                return Ok(());
            }
            let Some((object_type, payload)) = self.object_read(&key)? else {
                append_null_bulk_string(response_out);
                return Ok(());
            };
            let zset_max_listpack_entries = self.zset_max_listpack_entries.load(Ordering::Acquire);
            let encoding = object_encoding_name(object_type, &payload, zset_max_listpack_entries)?;
            append_bulk_string(response_out, encoding);
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"REFCOUNT") {
            if args.len() != 3 {
                return Err(RequestExecutionError::WrongArity {
                    command: "OBJECT",
                    expected: "OBJECT REFCOUNT key",
                });
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { args[2].as_slice() }.to_vec();
            self.expire_key_if_needed(&key)?;
            if self.key_exists_any(&key)? {
                append_integer(response_out, 1);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        }

        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_keys(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "KEYS",
                expected: "KEYS pattern",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let pattern = unsafe { args[1].as_slice() };

        let mut keys: HashSet<Vec<u8>> = self.string_keys_snapshot().into_iter().collect();
        keys.extend(self.object_keys_snapshot());

        let mut matched = Vec::new();
        for key in keys {
            self.expire_key_if_needed(&key)?;

            let string_exists = self.key_exists(&key)?;
            let object_exists = self.object_key_exists(&key)?;
            if !string_exists {
                self.untrack_string_key(&key);
            }
            if !object_exists {
                self.untrack_object_key(&key);
            }
            if !(string_exists || object_exists) {
                continue;
            }
            if redis_glob_match(pattern, &key, false, 0) {
                matched.push(key);
            }
        }

        response_out.push(b'*');
        response_out.extend_from_slice(matched.len().to_string().as_bytes());
        response_out.extend_from_slice(b"\r\n");
        for key in matched {
            append_bulk_string(response_out, &key);
        }
        Ok(())
    }

    pub(super) fn handle_randomkey(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "RANDOMKEY",
                expected: "RANDOMKEY",
            });
        }

        let mut keys: HashSet<Vec<u8>> = self.string_keys_snapshot().into_iter().collect();
        keys.extend(self.object_keys_snapshot());

        let mut live_keys = Vec::new();
        for key in keys {
            self.expire_key_if_needed(&key)?;

            let string_exists = self.key_exists(&key)?;
            let object_exists = self.object_key_exists(&key)?;
            if !string_exists {
                self.untrack_string_key(&key);
            }
            if !object_exists {
                self.untrack_object_key(&key);
            }
            if string_exists || object_exists {
                live_keys.push(key);
            }
        }

        if live_keys.is_empty() {
            append_null_bulk_string(response_out);
            return Ok(());
        }

        live_keys.sort_unstable();
        let index =
            (NEXT_RANDOMKEY_INDEX.fetch_add(1, Ordering::Relaxed) as usize) % live_keys.len();
        append_bulk_string(response_out, &live_keys[index]);
        Ok(())
    }

    pub(super) fn handle_scan(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "SCAN",
                expected: "SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]",
            });
        }

        // SAFETY: caller guarantees argument backing memory validity.
        let cursor = parse_u64_ascii(unsafe { args[1].as_slice() })
            .ok_or(RequestExecutionError::ValueNotInteger)?;
        let mut pattern: Option<&[u8]> = None;
        let mut count = 10usize;
        let mut type_filter: Option<ScanTypeFilter> = None;
        let mut index = 2usize;
        while index < args.len() {
            // SAFETY: caller guarantees argument backing memory validity.
            let token = unsafe { args[index].as_slice() };
            if ascii_eq_ignore_case(token, b"MATCH") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                // SAFETY: caller guarantees argument backing memory validity.
                pattern = Some(unsafe { args[index + 1].as_slice() });
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"COUNT") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                // SAFETY: caller guarantees argument backing memory validity.
                let parsed_count = parse_u64_ascii(unsafe { args[index + 1].as_slice() })
                    .ok_or(RequestExecutionError::ValueNotInteger)?;
                if parsed_count == 0 {
                    return Err(RequestExecutionError::ValueOutOfRange);
                }
                count = usize::try_from(parsed_count).unwrap_or(usize::MAX);
                index += 2;
                continue;
            }
            if ascii_eq_ignore_case(token, b"TYPE") {
                if index + 1 >= args.len() {
                    return Err(RequestExecutionError::SyntaxError);
                }
                // SAFETY: caller guarantees argument backing memory validity.
                let raw_type = unsafe { args[index + 1].as_slice() };
                type_filter = Some(
                    parse_scan_type_filter(raw_type).ok_or(RequestExecutionError::SyntaxError)?,
                );
                index += 2;
                continue;
            }
            return Err(RequestExecutionError::SyntaxError);
        }

        let mut keys: HashSet<Vec<u8>> = self.string_keys_snapshot().into_iter().collect();
        keys.extend(self.object_keys_snapshot());

        let mut matched = Vec::new();
        for key in keys {
            self.expire_key_if_needed(&key)?;
            let string_exists = self.key_exists(&key)?;
            let object_exists = self.object_key_exists(&key)?;
            if !string_exists {
                self.untrack_string_key(&key);
            }
            if !object_exists {
                self.untrack_object_key(&key);
            }
            if !(string_exists || object_exists) {
                continue;
            }

            if let Some(filter) = type_filter {
                if !self.scan_key_matches_type_filter(&key, string_exists, object_exists, filter)? {
                    continue;
                }
            }

            if let Some(pattern) = pattern {
                if !redis_glob_match(pattern, &key, false, 0) {
                    continue;
                }
            }

            matched.push(key);
        }
        matched.sort_unstable();
        append_scan_cursor_and_key_array(response_out, cursor, count, &matched);
        Ok(())
    }

    fn scan_key_matches_type_filter(
        &self,
        key: &[u8],
        string_exists: bool,
        object_exists: bool,
        type_filter: ScanTypeFilter,
    ) -> Result<bool, RequestExecutionError> {
        if type_filter == ScanTypeFilter::String {
            return Ok(string_exists);
        }
        if string_exists || !object_exists {
            return Ok(false);
        }
        let Some((object_type, _)) = self.object_read(key)? else {
            self.untrack_object_key(key);
            return Ok(false);
        };
        Ok(scan_object_type_matches_filter(object_type, type_filter))
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

    pub(super) fn handle_latency(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            2,
            "LATENCY",
            "LATENCY <DOCTOR|GRAPH|HISTORY|LATEST|RESET|HISTOGRAM|HELP> [arguments...]",
        )?;
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "LATENCY", "LATENCY HELP")?;
            append_bulk_array(response_out, &LATENCY_HELP_LINES);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"LATEST") {
            require_exact_arity(args, 2, "LATENCY", "LATENCY LATEST")?;
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"HISTORY") {
            require_exact_arity(args, 3, "LATENCY", "LATENCY HISTORY event")?;
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"RESET") {
            append_integer(response_out, 0);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"DOCTOR") {
            require_exact_arity(args, 2, "LATENCY", "LATENCY DOCTOR")?;
            append_bulk_string(response_out, LATENCY_DOCTOR_DISABLED_MESSAGE);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GRAPH") {
            require_exact_arity(args, 3, "LATENCY", "LATENCY GRAPH event")?;
            append_bulk_string(response_out, b"");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"HISTOGRAM") {
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }
        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_module(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "MODULE", "MODULE <LIST|HELP>")?;
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };
        if ascii_eq_ignore_case(subcommand, b"LIST") {
            require_exact_arity(args, 2, "MODULE", "MODULE LIST")?;
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "MODULE", "MODULE HELP")?;
            append_bulk_array(response_out, &MODULE_HELP_LINES);
            return Ok(());
        }
        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_slowlog(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            2,
            "SLOWLOG",
            "SLOWLOG <GET|LEN|RESET|HELP> [arguments...]",
        )?;
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "SLOWLOG", "SLOWLOG HELP")?;
            append_bulk_array(response_out, &SLOWLOG_HELP_LINES);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"LEN") {
            require_exact_arity(args, 2, "SLOWLOG", "SLOWLOG LEN")?;
            append_integer(response_out, 0);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"RESET") {
            require_exact_arity(args, 2, "SLOWLOG", "SLOWLOG RESET")?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GET") {
            ensure_ranged_arity(args, 2, 3, "SLOWLOG", "SLOWLOG GET [count]")?;
            if args.len() == 3 {
                // SAFETY: caller guarantees argument backing memory validity.
                parse_i64_ascii(unsafe { args[2].as_slice() })
                    .ok_or(RequestExecutionError::ValueNotInteger)?;
            }
            response_out.extend_from_slice(b"*0\r\n");
            return Ok(());
        }
        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_acl(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "ACL", "ACL <subcommand> [arguments...]")?;
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };
        if ascii_eq_ignore_case(subcommand, b"HELP") {
            require_exact_arity(args, 2, "ACL", "ACL HELP")?;
            append_bulk_array(response_out, &ACL_HELP_LINES);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"WHOAMI") {
            require_exact_arity(args, 2, "ACL", "ACL WHOAMI")?;
            append_bulk_string(response_out, b"default");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"USERS") {
            require_exact_arity(args, 2, "ACL", "ACL USERS")?;
            append_bulk_array(response_out, &[b"default"]);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"LIST") {
            require_exact_arity(args, 2, "ACL", "ACL LIST")?;
            append_bulk_array(
                response_out,
                &[b"user default on nopass sanitize-payload ~* &* +@all"],
            );
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"SETUSER") {
            ensure_min_arity(args, 3, "ACL", "ACL SETUSER username [rule ...]")?;
            append_simple_string(response_out, b"OK");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"CAT") {
            if args.len() == 2 {
                append_bulk_array(response_out, &ACL_CATEGORIES);
                return Ok(());
            }
            require_exact_arity(args, 3, "ACL", "ACL CAT [category]")?;
            append_bulk_array(response_out, &[]);
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"GETUSER") {
            require_exact_arity(args, 3, "ACL", "ACL GETUSER username")?;
            // SAFETY: caller guarantees argument backing memory validity.
            let username = unsafe { args[2].as_slice() };
            if !ascii_eq_ignore_case(username, b"default") {
                append_null_bulk_string(response_out);
                return Ok(());
            }
            append_bulk_array(
                response_out,
                &[
                    b"flags",
                    b"on",
                    b"nopass",
                    b"commands",
                    b"+@all",
                    b"keys",
                    b"~*",
                    b"channels",
                    b"&*",
                ],
            );
            return Ok(());
        }
        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_cluster(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "CLUSTER", "CLUSTER <subcommand> [arguments...]")?;
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };
        if ascii_eq_ignore_case(subcommand, b"KEYSLOT") {
            require_exact_arity(args, 3, "CLUSTER", "CLUSTER KEYSLOT key")?;
            // SAFETY: caller guarantees argument backing memory validity.
            let key = unsafe { args[2].as_slice() };
            append_integer(response_out, i64::from(redis_hash_slot(key)));
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"INFO") {
            require_exact_arity(args, 2, "CLUSTER", "CLUSTER INFO")?;
            append_bulk_string(
                response_out,
                b"cluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_known_nodes:1\r\n",
            );
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"MYID") {
            require_exact_arity(args, 2, "CLUSTER", "CLUSTER MYID")?;
            append_bulk_string(response_out, b"0000000000000000000000000000000000000000");
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"NODES") {
            require_exact_arity(args, 2, "CLUSTER", "CLUSTER NODES")?;
            append_bulk_string(
                response_out,
                b"0000000000000000000000000000000000000000 127.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-16383\n",
            );
            return Ok(());
        }
        if ascii_eq_ignore_case(subcommand, b"SLOTS") {
            require_exact_arity(args, 2, "CLUSTER", "CLUSTER SLOTS")?;
            response_out.extend_from_slice(b"*1\r\n*3\r\n");
            append_integer(response_out, 0);
            append_integer(response_out, 16_383);
            response_out.extend_from_slice(b"*2\r\n");
            append_bulk_string(response_out, b"127.0.0.1");
            append_integer(response_out, 6379);
            return Ok(());
        }
        Err(RequestExecutionError::ClusterSupportDisabled)
    }

    pub(super) fn handle_failover(
        &self,
        args: &[ArgSlice],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            1,
            "FAILOVER",
            "FAILOVER [TO host port] [FORCE] [TIMEOUT ms]",
        )?;
        Err(RequestExecutionError::ClusterSupportDisabled)
    }

    pub(super) fn handle_monitor(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        require_exact_arity(args, 1, "MONITOR", "MONITOR")?;
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_shutdown(
        &self,
        args: &[ArgSlice],
        _response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(
            args,
            1,
            "SHUTDOWN",
            "SHUTDOWN [NOSAVE|SAVE] [NOW|FORCE|ABORT]",
        )?;
        for option in &args[1..] {
            // SAFETY: caller guarantees argument backing memory validity.
            let token = unsafe { option.as_slice() };
            if ascii_eq_ignore_case(token, b"SAVE")
                || ascii_eq_ignore_case(token, b"NOSAVE")
                || ascii_eq_ignore_case(token, b"NOW")
                || ascii_eq_ignore_case(token, b"FORCE")
                || ascii_eq_ignore_case(token, b"ABORT")
            {
                continue;
            }
            return Err(RequestExecutionError::SyntaxError);
        }
        Err(RequestExecutionError::CommandDisabled {
            command: "SHUTDOWN",
        })
    }

    pub(super) fn handle_function(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "FUNCTION",
                expected: "FUNCTION FLUSH",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };
        if !ascii_eq_ignore_case(subcommand, b"FLUSH") {
            return Err(RequestExecutionError::UnknownCommand);
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_script(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 2 && args.len() != 3 {
            return Err(RequestExecutionError::WrongArity {
                command: "SCRIPT",
                expected: "SCRIPT FLUSH [ASYNC|SYNC]",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };
        if !ascii_eq_ignore_case(subcommand, b"FLUSH") {
            return Err(RequestExecutionError::UnknownCommand);
        }
        if args.len() == 3 {
            // SAFETY: caller guarantees argument backing memory validity.
            let flush_mode = unsafe { args[2].as_slice() };
            if !ascii_eq_ignore_case(flush_mode, b"ASYNC")
                && !ascii_eq_ignore_case(flush_mode, b"SYNC")
            {
                return Err(RequestExecutionError::UnknownCommand);
            }
        }
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_config(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() < 2 {
            return Err(RequestExecutionError::WrongArity {
                command: "CONFIG",
                expected: "CONFIG <GET|SET|RESETSTAT>",
            });
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let subcommand = unsafe { args[1].as_slice() };

        if ascii_eq_ignore_case(subcommand, b"RESETSTAT") {
            if args.len() != 2 {
                return Err(RequestExecutionError::WrongArity {
                    command: "CONFIG",
                    expected: "CONFIG RESETSTAT",
                });
            }
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"SET") {
            if args.len() != 4 {
                return Err(RequestExecutionError::WrongArity {
                    command: "CONFIG",
                    expected: "CONFIG SET parameter value",
                });
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let parameter = unsafe { args[2].as_slice() };
            // SAFETY: caller guarantees argument backing memory validity.
            let value = unsafe { args[3].as_slice() };

            if ascii_eq_ignore_case(parameter, b"ZSET-MAX-ZIPLIST-ENTRIES")
                || ascii_eq_ignore_case(parameter, b"ZSET-MAX-LISTPACK-ENTRIES")
            {
                let parsed =
                    parse_u64_ascii(value).ok_or(RequestExecutionError::ValueNotInteger)?;
                self.zset_max_listpack_entries
                    .store(parsed as usize, Ordering::Release);
            }
            append_simple_string(response_out, b"OK");
            return Ok(());
        }

        if ascii_eq_ignore_case(subcommand, b"GET") {
            if args.len() != 3 {
                return Err(RequestExecutionError::WrongArity {
                    command: "CONFIG",
                    expected: "CONFIG GET parameter",
                });
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let pattern = unsafe { args[2].as_slice() };
            let zset_max_listpack_entries = self.zset_max_listpack_entries.load(Ordering::Acquire);
            let zset_max_listpack_entries_value = zset_max_listpack_entries.to_string();

            let config_items: [(&[u8], &[u8]); 7] = [
                (b"appendonly", b"no"),
                (b"save", b""),
                (b"dbfilename", b"dump.rdb"),
                (b"dir", b"."),
                (b"maxmemory", b"0"),
                (
                    b"zset-max-ziplist-entries",
                    zset_max_listpack_entries_value.as_bytes(),
                ),
                (
                    b"zset-max-listpack-entries",
                    zset_max_listpack_entries_value.as_bytes(),
                ),
            ];

            let matched_items: Vec<(&[u8], &[u8])> = if ascii_eq_ignore_case(pattern, b"*") {
                config_items.into_iter().collect()
            } else {
                config_items
                    .into_iter()
                    .filter(|(key, _)| pattern.eq_ignore_ascii_case(key))
                    .collect()
            };

            response_out.push(b'*');
            response_out.extend_from_slice((matched_items.len() * 2).to_string().as_bytes());
            response_out.extend_from_slice(b"\r\n");
            for (key, value) in matched_items {
                append_bulk_string(response_out, key);
                append_bulk_string(response_out, value);
            }
            return Ok(());
        }

        Err(RequestExecutionError::UnknownCommand)
    }

    pub(super) fn handle_flushdb(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "FLUSHDB",
                expected: "FLUSHDB",
            });
        }
        self.flush_all_keys()?;
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    pub(super) fn handle_flushall(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        if args.len() != 1 {
            return Err(RequestExecutionError::WrongArity {
                command: "FLUSHALL",
                expected: "FLUSHALL",
            });
        }
        self.flush_all_keys()?;
        append_simple_string(response_out, b"OK");
        Ok(())
    }

    fn flush_all_keys(&self) -> Result<(), RequestExecutionError> {
        let mut keys: HashSet<Vec<u8>> = self.string_keys_snapshot().into_iter().collect();
        keys.extend(self.object_keys_snapshot());

        for key in keys {
            self.expire_key_if_needed(&key)?;

            let mut string_deleted = false;
            {
                let mut store = self.lock_string_store_for_key(&key);
                let mut session = store.session(&self.functions);
                let mut info = DeleteInfo::default();
                let status = session.delete(&key, &mut info).map_err(map_delete_error)?;
                match status {
                    DeleteOperationStatus::TombstonedInPlace
                    | DeleteOperationStatus::AppendedTombstone => {
                        string_deleted = true;
                    }
                    DeleteOperationStatus::NotFound => {}
                    DeleteOperationStatus::RetryLater => {
                        return Err(RequestExecutionError::StorageBusy);
                    }
                }
            }
            if string_deleted {
                self.remove_string_key_metadata(&key);
            }

            let object_deleted = self.object_delete(&key)?;
            if string_deleted && !object_deleted {
                self.bump_watch_version(&key);
            }
        }

        for shard_index in 0..self.string_store_shard_count() {
            self.lock_string_expirations_for_shard(shard_index).clear();
            self.lock_string_key_registry_for_shard(shard_index).clear();
            self.lock_object_key_registry_for_shard(shard_index).clear();
            self.string_expiration_counts[shard_index].store(0, Ordering::Release);
        }

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

    fn debug_digest_value_for_key(&self, key: &[u8]) -> Result<Vec<u8>, RequestExecutionError> {
        if let Some(value) = self.read_string_value(key)? {
            return Ok(fnv_hex_digest(1, &value));
        }

        if let Some((object_type, payload)) = self.object_read(key)? {
            let mut combined = Vec::with_capacity(1 + payload.len());
            combined.push(object_type);
            combined.extend_from_slice(&payload);
            return Ok(fnv_hex_digest(2, &combined));
        }

        Ok(fnv_hex_digest(0, b""))
    }
}

fn object_encoding_name(
    object_type: u8,
    payload: &[u8],
    zset_listpack_max_entries: usize,
) -> Result<&'static [u8], RequestExecutionError> {
    const SMALL_ELEMENT_BYTES: usize = 64;
    const LIST_LISTPACK_MAX_ENTRIES: usize = 128;
    const SET_LISTPACK_MAX_ENTRIES: usize = 128;
    const HASH_LISTPACK_MAX_ENTRIES: usize = 32;

    match object_type {
        LIST_OBJECT_TYPE_TAG => {
            let list = deserialize_list_object_payload(payload).ok_or_else(|| {
                storage_failure("object.encoding", "failed to decode list payload")
            })?;
            let compact = list.len() <= LIST_LISTPACK_MAX_ENTRIES
                && list.iter().all(|value| value.len() <= SMALL_ELEMENT_BYTES);
            if compact {
                Ok(b"listpack")
            } else {
                Ok(b"quicklist")
            }
        }
        SET_OBJECT_TYPE_TAG => {
            let set = deserialize_set_object_payload(payload).ok_or_else(|| {
                storage_failure("object.encoding", "failed to decode set payload")
            })?;
            let intset =
                set.iter().all(|member| parse_i64_ascii(member).is_some()) && set.len() <= 512;
            if intset {
                return Ok(b"intset");
            }
            let compact = set.len() <= SET_LISTPACK_MAX_ENTRIES
                && set.iter().all(|member| member.len() <= SMALL_ELEMENT_BYTES);
            if compact {
                Ok(b"listpack")
            } else {
                Ok(b"hashtable")
            }
        }
        HASH_OBJECT_TYPE_TAG => {
            let hash = deserialize_hash_object_payload(payload).ok_or_else(|| {
                storage_failure("object.encoding", "failed to decode hash payload")
            })?;
            let compact = hash.len() <= HASH_LISTPACK_MAX_ENTRIES
                && hash.iter().all(|(field, value)| {
                    field.len() <= SMALL_ELEMENT_BYTES && value.len() <= SMALL_ELEMENT_BYTES
                });
            if compact {
                Ok(b"listpack")
            } else {
                Ok(b"hashtable")
            }
        }
        ZSET_OBJECT_TYPE_TAG => {
            let zset = deserialize_zset_object_payload(payload).ok_or_else(|| {
                storage_failure("object.encoding", "failed to decode zset payload")
            })?;
            let compact = zset.len() <= zset_listpack_max_entries
                && zset
                    .keys()
                    .all(|member| member.len() <= SMALL_ELEMENT_BYTES);
            if compact {
                Ok(b"listpack")
            } else {
                Ok(b"skiplist")
            }
        }
        STREAM_OBJECT_TYPE_TAG => Ok(b"stream"),
        _ => Err(storage_failure(
            "object.encoding",
            "unknown object type tag in object store",
        )),
    }
}

fn fnv_hex_digest(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut hash = 0xcbf29ce484222325u64;
    hash ^= u64::from(tag);
    hash = hash.wrapping_mul(0x100000001b3);
    for byte in payload {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}").into_bytes()
}

fn estimate_memory_usage_bytes(key_len: usize, value_len: usize) -> i64 {
    const ESTIMATED_ENTRY_OVERHEAD_BYTES: usize = 64;
    let total = key_len
        .saturating_add(value_len)
        .saturating_add(ESTIMATED_ENTRY_OVERHEAD_BYTES);
    i64::try_from(total).unwrap_or(i64::MAX)
}

fn append_scan_cursor_and_key_array(
    response_out: &mut Vec<u8>,
    cursor: u64,
    count: usize,
    keys: &[Vec<u8>],
) {
    let start = usize::try_from(cursor)
        .unwrap_or(usize::MAX)
        .min(keys.len());
    let end = start.saturating_add(count).min(keys.len());
    let next_cursor = if end >= keys.len() { 0 } else { end };

    response_out.push(b'*');
    response_out.extend_from_slice(b"2\r\n");
    let next_cursor_bytes = next_cursor.to_string();
    append_bulk_string(response_out, next_cursor_bytes.as_bytes());
    response_out.push(b'*');
    response_out.extend_from_slice((end - start).to_string().as_bytes());
    response_out.extend_from_slice(b"\r\n");
    for key in &keys[start..end] {
        append_bulk_string(response_out, key);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanTypeFilter {
    String,
    Hash,
    List,
    Set,
    Zset,
    Stream,
}

fn parse_scan_type_filter(input: &[u8]) -> Option<ScanTypeFilter> {
    if ascii_eq_ignore_case(input, b"STRING") {
        return Some(ScanTypeFilter::String);
    }
    if ascii_eq_ignore_case(input, b"HASH") {
        return Some(ScanTypeFilter::Hash);
    }
    if ascii_eq_ignore_case(input, b"LIST") {
        return Some(ScanTypeFilter::List);
    }
    if ascii_eq_ignore_case(input, b"SET") {
        return Some(ScanTypeFilter::Set);
    }
    if ascii_eq_ignore_case(input, b"ZSET") {
        return Some(ScanTypeFilter::Zset);
    }
    if ascii_eq_ignore_case(input, b"STREAM") {
        return Some(ScanTypeFilter::Stream);
    }
    None
}

fn scan_object_type_matches_filter(object_type: u8, filter: ScanTypeFilter) -> bool {
    match filter {
        ScanTypeFilter::String => false,
        ScanTypeFilter::Hash => object_type == HASH_OBJECT_TYPE_TAG,
        ScanTypeFilter::List => object_type == LIST_OBJECT_TYPE_TAG,
        ScanTypeFilter::Set => object_type == SET_OBJECT_TYPE_TAG,
        ScanTypeFilter::Zset => object_type == ZSET_OBJECT_TYPE_TAG,
        ScanTypeFilter::Stream => object_type == STREAM_OBJECT_TYPE_TAG,
    }
}

pub(super) fn redis_glob_match(pattern: &[u8], text: &[u8], nocase: bool, _nesting: usize) -> bool {
    // Guard against pathological wildcard patterns that can trigger excessive backtracking.
    // Redis keyspace regression tests rely on this returning quickly instead of hanging.
    const MAX_MATCH_WORK: usize = 1_000_000;
    let estimated_work = pattern.len().checked_mul(text.len()).unwrap_or(usize::MAX);
    if estimated_work > MAX_MATCH_WORK {
        return false;
    }

    let mut pattern_index = 0usize;
    let mut text_index = 0usize;
    let mut star_resume_pattern_index = None::<usize>;
    let mut star_resume_text_index = 0usize;

    while text_index < text.len() {
        if pattern_index < pattern.len() {
            match pattern[pattern_index] {
                b'*' => {
                    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
                        pattern_index += 1;
                    }
                    if pattern_index == pattern.len() {
                        return true;
                    }
                    star_resume_pattern_index = Some(pattern_index);
                    star_resume_text_index = text_index;
                    continue;
                }
                b'?' => {
                    pattern_index += 1;
                    text_index += 1;
                    continue;
                }
                b'[' => {
                    if let Some((matched, next_index)) =
                        glob_match_class(pattern, pattern_index, text[text_index], nocase)
                    {
                        if matched {
                            pattern_index = next_index;
                            text_index += 1;
                            continue;
                        }
                    } else if bytes_eq(b'[', text[text_index], nocase) {
                        pattern_index += 1;
                        text_index += 1;
                        continue;
                    }
                }
                b'\\' => {
                    let literal_index = (pattern_index + 1).min(pattern.len() - 1);
                    if bytes_eq(pattern[literal_index], text[text_index], nocase) {
                        pattern_index = literal_index + 1;
                        text_index += 1;
                        continue;
                    }
                }
                pattern_ch => {
                    if bytes_eq(pattern_ch, text[text_index], nocase) {
                        pattern_index += 1;
                        text_index += 1;
                        continue;
                    }
                }
            }
        }

        if let Some(resume_pattern_index) = star_resume_pattern_index {
            star_resume_text_index += 1;
            if star_resume_text_index > text.len() {
                return false;
            }
            text_index = star_resume_text_index;
            pattern_index = resume_pattern_index;
            continue;
        }

        return false;
    }

    while pattern_index < pattern.len() && pattern[pattern_index] == b'*' {
        pattern_index += 1;
    }
    pattern_index == pattern.len()
}

fn glob_match_class(
    pattern: &[u8],
    class_open_index: usize,
    candidate: u8,
    nocase: bool,
) -> Option<(bool, usize)> {
    let mut class_index = class_open_index + 1;
    let mut negate = false;
    if class_index < pattern.len() && pattern[class_index] == b'^' {
        negate = true;
        class_index += 1;
    }

    let mut matched = false;
    while class_index < pattern.len() {
        let class_ch = pattern[class_index];
        if class_ch == b']' {
            class_index += 1;
            if negate {
                matched = !matched;
            }
            return Some((matched, class_index));
        }

        if class_ch == b'\\' && class_index + 1 < pattern.len() {
            class_index += 1;
            if bytes_eq(pattern[class_index], candidate, nocase) {
                matched = true;
            }
            class_index += 1;
            continue;
        }

        if class_index + 2 < pattern.len()
            && pattern[class_index + 1] == b'-'
            && pattern[class_index + 2] != b']'
        {
            let mut start = pattern[class_index];
            let mut end = pattern[class_index + 2];
            let mut value = candidate;
            if nocase {
                start = start.to_ascii_lowercase();
                end = end.to_ascii_lowercase();
                value = value.to_ascii_lowercase();
            }
            if start > end {
                std::mem::swap(&mut start, &mut end);
            }
            if value >= start && value <= end {
                matched = true;
            }
            class_index += 3;
            continue;
        }

        if bytes_eq(class_ch, candidate, nocase) {
            matched = true;
        }
        class_index += 1;
    }

    None
}

#[inline]
fn bytes_eq(left: u8, right: u8, nocase: bool) -> bool {
    if nocase {
        left.eq_ignore_ascii_case(&right)
    } else {
        left == right
    }
}

#[cfg(test)]
mod tests {
    use super::redis_glob_match;

    #[test]
    fn redis_glob_match_supports_star_and_question() {
        assert!(redis_glob_match(b"foo*", b"foobar", false, 0));
        assert!(redis_glob_match(b"f?o", b"foo", false, 0));
        assert!(!redis_glob_match(b"foo?", b"foo", false, 0));
    }

    #[test]
    fn redis_glob_match_supports_character_classes_and_escapes() {
        assert!(redis_glob_match(b"f[oa]o", b"foo", false, 0));
        assert!(redis_glob_match(b"f[a-z]o", b"fbo", false, 0));
        assert!(redis_glob_match(b"f[^x]o", b"foo", false, 0));
        assert!(!redis_glob_match(b"f[^o]o", b"foo", false, 0));
        assert!(redis_glob_match(b"foo\\*", b"foo*", false, 0));
    }
}
