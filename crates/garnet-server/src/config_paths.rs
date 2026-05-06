use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;

const DEFAULT_CONFIG_FILE_DIR: &str = ".";

pub(crate) fn default_config_dir_value() -> Vec<u8> {
    default_config_dir_path()
        .to_string_lossy()
        .into_owned()
        .into_bytes()
}

pub(crate) fn normalize_config_dir_value(value: &[u8]) -> Vec<u8> {
    config_dir_path_from_bytes(value)
        .to_string_lossy()
        .into_owned()
        .into_bytes()
}

pub(crate) fn normalize_config_dir_path(path: &Path) -> Vec<u8> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        default_config_dir_path().join(path)
    };
    absolute.to_string_lossy().into_owned().into_bytes()
}

pub(crate) fn optional_configured_file_path_from_values(
    values: &HashMap<Vec<u8>, Vec<u8>>,
    file_key: &[u8],
) -> Option<PathBuf> {
    let file_value = values.get(file_key)?;
    if file_value.is_empty() {
        return None;
    }

    Some(configured_file_path_from_parts(
        values.get(b"dir".as_slice()).map(Vec::as_slice),
        file_value,
    ))
}

pub(crate) fn configured_file_path_from_values(
    values: &HashMap<Vec<u8>, Vec<u8>>,
    file_key: &[u8],
    default_file_name: &[u8],
) -> PathBuf {
    let file_value = values
        .get(file_key)
        .map(Vec::as_slice)
        .unwrap_or(default_file_name);
    configured_file_path_from_parts(values.get(b"dir".as_slice()).map(Vec::as_slice), file_value)
}

pub(crate) fn configured_file_path_from_config_items(
    config_items: &[(Vec<u8>, Vec<u8>)],
    file_key: &[u8],
    default_file_name: &[u8],
) -> PathBuf {
    let mut dir_value = None::<&[u8]>;
    let mut file_value = None::<&[u8]>;
    for (key, value) in config_items {
        if key.as_slice() == b"dir" {
            dir_value = Some(value.as_slice());
            continue;
        }
        if key.as_slice() == file_key {
            file_value = Some(value.as_slice());
        }
    }

    configured_file_path_from_parts(dir_value, file_value.unwrap_or(default_file_name))
}

fn configured_file_path_from_parts(config_dir: Option<&[u8]>, file_value: &[u8]) -> PathBuf {
    let file_path = path_buf_from_config_value(file_value);
    if file_path.is_absolute() {
        return file_path;
    }

    let mut path = config_dir
        .map(config_dir_path_from_bytes)
        .unwrap_or_else(default_config_dir_path);
    path.push(file_path);
    path
}

fn config_dir_path_from_bytes(value: &[u8]) -> PathBuf {
    let path = path_buf_from_config_value(value);
    if path.is_absolute() {
        return path;
    }

    default_config_dir_path().join(path)
}

fn default_config_dir_path() -> PathBuf {
    std::env::current_dir().unwrap_or_else(|_| PathBuf::from(DEFAULT_CONFIG_FILE_DIR))
}

fn path_buf_from_config_value(value: &[u8]) -> PathBuf {
    PathBuf::from(String::from_utf8_lossy(value).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_path_normalize_dir_value_makes_relative_paths_absolute() {
        let expected = std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("relative-dir")
            .to_string_lossy()
            .into_owned()
            .into_bytes();

        assert_eq!(normalize_config_dir_value(b"relative-dir"), expected);
    }

    #[test]
    fn config_path_normalize_dir_path_makes_relative_paths_absolute() {
        let expected = std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("relative-dir")
            .to_string_lossy()
            .into_owned()
            .into_bytes();

        assert_eq!(
            normalize_config_dir_path(Path::new("relative-dir")),
            expected
        );
    }

    #[test]
    fn config_path_optional_file_path_resolves_relative_and_absolute_inputs() {
        let mut values = HashMap::new();
        values.insert(b"dir".to_vec(), b"/tmp/garnet-config".to_vec());
        values.insert(b"aclfile".to_vec(), b"users.acl".to_vec());

        assert_eq!(
            optional_configured_file_path_from_values(&values, b"aclfile"),
            Some(PathBuf::from("/tmp/garnet-config/users.acl"))
        );

        values.insert(
            b"aclfile".to_vec(),
            b"/var/lib/garnet/absolute-users.acl".to_vec(),
        );
        assert_eq!(
            optional_configured_file_path_from_values(&values, b"aclfile"),
            Some(PathBuf::from("/var/lib/garnet/absolute-users.acl"))
        );
    }

    #[test]
    fn config_path_from_config_items_uses_defaults_when_values_are_missing() {
        let expected = std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("dump.rdb");

        assert_eq!(
            configured_file_path_from_config_items(&[], b"dbfilename", b"dump.rdb"),
            expected
        );
    }
}
