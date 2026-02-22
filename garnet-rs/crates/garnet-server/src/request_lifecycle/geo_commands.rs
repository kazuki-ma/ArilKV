use super::*;

const GEOADD_USAGE: &str =
    "GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]";
const GEO_LONGITUDE_MIN: f64 = -180.0;
const GEO_LONGITUDE_MAX: f64 = 180.0;
const GEO_LATITUDE_MIN: f64 = -85.051_128_78;
const GEO_LATITUDE_MAX: f64 = 85.051_128_78;
const GEO_COORD_BITS: u32 = 26;
const GEO_COORD_MASK: u64 = (1u64 << GEO_COORD_BITS) - 1;
const GEO_SCORE_MAX: u64 = (GEO_COORD_MASK << GEO_COORD_BITS) | GEO_COORD_MASK;

#[derive(Default)]
struct GeoAddOptions {
    nx: bool,
    xx: bool,
    ch: bool,
}

impl RequestProcessor {
    pub(super) fn handle_geoadd(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 5, "GEOADD", GEOADD_USAGE)?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() }.to_vec();
        let (options, mut index) = parse_geoadd_options(args, 2)?;

        let remaining = args.len().saturating_sub(index);
        if remaining == 0 || remaining % 3 != 0 {
            return Err(RequestExecutionError::WrongArity {
                command: "GEOADD",
                expected: GEOADD_USAGE,
            });
        }

        let mut zset = self.load_zset_object(&key)?.unwrap_or_default();
        let mut updated = false;
        let mut added = 0i64;
        let mut changed = 0i64;

        while index + 2 < args.len() {
            // SAFETY: caller guarantees argument backing memory validity.
            let longitude = parse_f64_ascii(unsafe { args[index].as_slice() })
                .ok_or(RequestExecutionError::ValueNotFloat)?;
            // SAFETY: caller guarantees argument backing memory validity.
            let latitude = parse_f64_ascii(unsafe { args[index + 1].as_slice() })
                .ok_or(RequestExecutionError::ValueNotFloat)?;
            if !geo_coordinates_in_range(longitude, latitude) {
                return Err(RequestExecutionError::ValueOutOfRange);
            }

            // SAFETY: caller guarantees argument backing memory validity.
            let member = unsafe { args[index + 2].as_slice() };
            let previous = zset.get(member).copied();

            if options.nx && previous.is_some() {
                index += 3;
                continue;
            }
            if options.xx && previous.is_none() {
                index += 3;
                continue;
            }

            let score = encode_geo_score(longitude, latitude);
            let needs_update = match previous {
                Some(existing) => existing != score,
                None => true,
            };
            if previous.is_none() {
                added += 1;
                changed += 1;
            } else if needs_update {
                changed += 1;
            }

            if needs_update {
                zset.insert(member.to_vec(), score);
                updated = true;
            }

            index += 3;
        }

        if updated {
            self.save_zset_object(&key, &zset)?;
        }

        let result = if options.ch { changed } else { added };
        append_integer(response_out, result);
        Ok(())
    }
}

fn parse_geoadd_options(
    args: &[ArgSlice],
    mut index: usize,
) -> Result<(GeoAddOptions, usize), RequestExecutionError> {
    let mut options = GeoAddOptions::default();
    while index < args.len() {
        // SAFETY: caller guarantees argument backing memory validity.
        let token = unsafe { args[index].as_slice() };
        if ascii_eq_ignore_case(token, b"NX") {
            options.nx = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"XX") {
            options.xx = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"CH") {
            options.ch = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"GT") || ascii_eq_ignore_case(token, b"LT") {
            return Err(RequestExecutionError::SyntaxError);
        }
        break;
    }
    if options.nx && options.xx {
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok((options, index))
}

fn geo_coordinates_in_range(longitude: f64, latitude: f64) -> bool {
    (GEO_LONGITUDE_MIN..=GEO_LONGITUDE_MAX).contains(&longitude)
        && (GEO_LATITUDE_MIN..=GEO_LATITUDE_MAX).contains(&latitude)
}

fn encode_geo_score(longitude: f64, latitude: f64) -> f64 {
    let lon_bits =
        quantize_geo_coordinate(longitude, GEO_LONGITUDE_MIN, GEO_LONGITUDE_MAX) & GEO_COORD_MASK;
    let lat_bits =
        quantize_geo_coordinate(latitude, GEO_LATITUDE_MIN, GEO_LATITUDE_MAX) & GEO_COORD_MASK;
    let packed = (lon_bits << GEO_COORD_BITS) | lat_bits;
    debug_assert!(packed <= GEO_SCORE_MAX);
    packed as f64
}

fn quantize_geo_coordinate(value: f64, min: f64, max: f64) -> u64 {
    let normalized = ((value - min) / (max - min)).clamp(0.0, 1.0);
    (normalized * GEO_COORD_MASK as f64).round() as u64
}
