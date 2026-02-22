use super::*;

const GEOADD_USAGE: &str =
    "GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]";
const GEOPOS_USAGE: &str = "GEOPOS key member [member ...]";
const GEODIST_USAGE: &str = "GEODIST key member1 member2 [M|KM|FT|MI]";
const GEO_LONGITUDE_MIN: f64 = -180.0;
const GEO_LONGITUDE_MAX: f64 = 180.0;
const GEO_LATITUDE_MIN: f64 = -85.051_128_78;
const GEO_LATITUDE_MAX: f64 = 85.051_128_78;
const GEO_COORD_BITS: u32 = 26;
const GEO_COORD_MASK: u64 = (1u64 << GEO_COORD_BITS) - 1;
const GEO_SCORE_MAX: u64 = (GEO_COORD_MASK << GEO_COORD_BITS) | GEO_COORD_MASK;
const GEO_EARTH_RADIUS_METERS: f64 = 6_372_797.560_856;

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

    pub(super) fn handle_geopos(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "GEOPOS", GEOPOS_USAGE)?;

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() };
        let zset = self.load_zset_object(key)?;

        response_out.extend_from_slice(format!("*{}\r\n", args.len() - 2).as_bytes());
        for member in &args[2..] {
            // SAFETY: caller guarantees argument backing memory validity.
            let member = unsafe { member.as_slice() };
            let Some(score) = zset.as_ref().and_then(|entries| entries.get(member)) else {
                append_null_bulk_string(response_out);
                continue;
            };
            let Some((longitude, latitude)) = decode_geo_score(*score) else {
                append_null_bulk_string(response_out);
                continue;
            };

            response_out.extend_from_slice(b"*2\r\n");
            let longitude_text = format_geo_coordinate(longitude);
            let latitude_text = format_geo_coordinate(latitude);
            append_bulk_string(response_out, longitude_text.as_bytes());
            append_bulk_string(response_out, latitude_text.as_bytes());
        }
        Ok(())
    }

    pub(super) fn handle_geodist(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(args, 4, 5, "GEODIST", GEODIST_USAGE)?;

        let unit_to_meters = if args.len() == 5 {
            // SAFETY: caller guarantees argument backing memory validity.
            let unit = unsafe { args[4].as_slice() };
            parse_geodist_unit_to_meters(unit)?
        } else {
            1.0
        };

        // SAFETY: caller guarantees argument backing memory validity.
        let key = unsafe { args[1].as_slice() };
        let zset = match self.load_zset_object(key)? {
            Some(entries) => entries,
            None => {
                append_null_bulk_string(response_out);
                return Ok(());
            }
        };

        // SAFETY: caller guarantees argument backing memory validity.
        let member_a = unsafe { args[2].as_slice() };
        // SAFETY: caller guarantees argument backing memory validity.
        let member_b = unsafe { args[3].as_slice() };
        let Some(score_a) = zset.get(member_a).copied() else {
            append_null_bulk_string(response_out);
            return Ok(());
        };
        let Some(score_b) = zset.get(member_b).copied() else {
            append_null_bulk_string(response_out);
            return Ok(());
        };

        let Some((lon_a, lat_a)) = decode_geo_score(score_a) else {
            append_null_bulk_string(response_out);
            return Ok(());
        };
        let Some((lon_b, lat_b)) = decode_geo_score(score_b) else {
            append_null_bulk_string(response_out);
            return Ok(());
        };

        let meters = geo_distance_meters(lon_a, lat_a, lon_b, lat_b);
        let unit_value = meters / unit_to_meters;
        let text = format_geo_number(unit_value, 4);
        append_bulk_string(response_out, text.as_bytes());
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

fn parse_geodist_unit_to_meters(unit: &[u8]) -> Result<f64, RequestExecutionError> {
    if ascii_eq_ignore_case(unit, b"M") {
        return Ok(1.0);
    }
    if ascii_eq_ignore_case(unit, b"KM") {
        return Ok(1000.0);
    }
    if ascii_eq_ignore_case(unit, b"FT") {
        return Ok(0.3048);
    }
    if ascii_eq_ignore_case(unit, b"MI") {
        return Ok(1609.34);
    }
    Err(RequestExecutionError::UnsupportedUnit)
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

fn decode_geo_score(score: f64) -> Option<(f64, f64)> {
    if !score.is_finite() || score < 0.0 || score > GEO_SCORE_MAX as f64 {
        return None;
    }
    if score.fract() != 0.0 {
        return None;
    }
    let packed = score as u64;
    let lon_bits = (packed >> GEO_COORD_BITS) & GEO_COORD_MASK;
    let lat_bits = packed & GEO_COORD_MASK;
    let longitude = dequantize_geo_coordinate(lon_bits, GEO_LONGITUDE_MIN, GEO_LONGITUDE_MAX);
    let latitude = dequantize_geo_coordinate(lat_bits, GEO_LATITUDE_MIN, GEO_LATITUDE_MAX);
    Some((longitude, latitude))
}

fn quantize_geo_coordinate(value: f64, min: f64, max: f64) -> u64 {
    let normalized = ((value - min) / (max - min)).clamp(0.0, 1.0);
    (normalized * GEO_COORD_MASK as f64).round() as u64
}

fn dequantize_geo_coordinate(value: u64, min: f64, max: f64) -> f64 {
    let normalized = value as f64 / GEO_COORD_MASK as f64;
    min + (max - min) * normalized
}

fn geo_distance_meters(lon_a: f64, lat_a: f64, lon_b: f64, lat_b: f64) -> f64 {
    let lat_a = lat_a.to_radians();
    let lat_b = lat_b.to_radians();
    let delta_lat = lat_b - lat_a;
    let delta_lon = (lon_b - lon_a).to_radians();
    let sin_lat = (delta_lat / 2.0).sin();
    let sin_lon = (delta_lon / 2.0).sin();
    let a = sin_lat * sin_lat + lat_a.cos() * lat_b.cos() * sin_lon * sin_lon;
    let c = 2.0 * a.sqrt().asin();
    GEO_EARTH_RADIUS_METERS * c
}

fn format_geo_coordinate(value: f64) -> String {
    format_geo_number(value, 17)
}

fn format_geo_number(value: f64, precision: usize) -> String {
    let mut text = format!("{value:.precision$}");
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.push('0');
        }
    }
    text
}
