use super::*;
use std::cmp::Ordering;

const GEOADD_USAGE: &str =
    "GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]";
const GEOPOS_USAGE: &str = "GEOPOS key member [member ...]";
const GEODIST_USAGE: &str = "GEODIST key member1 member2 [M|KM|FT|MI]";
const GEOHASH_USAGE: &str = "GEOHASH key member [member ...]";
const GEORADIUS_USAGE: &str =
    "GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]";
const GEORADIUSBYMEMBER_USAGE: &str =
    "GEORADIUSBYMEMBER key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]";
const GEORADIUS_RO_USAGE: &str =
    "GEORADIUS_RO key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC]";
const GEORADIUSBYMEMBER_RO_USAGE: &str =
    "GEORADIUSBYMEMBER_RO key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC]";
const GEOSEARCH_USAGE: &str =
    "GEOSEARCH key FROMMEMBER member|FROMLONLAT lon lat BYRADIUS radius unit|BYBOX width height unit [options]";
const GEOSEARCHSTORE_USAGE: &str =
    "GEOSEARCHSTORE destination source FROMMEMBER member|FROMLONLAT lon lat BYRADIUS radius unit|BYBOX width height unit [options]";
const GEO_LONGITUDE_MIN: f64 = -180.0;
const GEO_LONGITUDE_MAX: f64 = 180.0;
const GEO_LATITUDE_MIN: f64 = -85.051_128_78;
const GEO_LATITUDE_MAX: f64 = 85.051_128_78;
const STANDARD_GEOHASH_LAT_MIN: f64 = -90.0;
const STANDARD_GEOHASH_LAT_MAX: f64 = 90.0;
const GEO_COORD_BITS: u32 = 26;
const GEO_COORD_MASK: u64 = (1u64 << GEO_COORD_BITS) - 1;
const GEO_SCORE_MAX: u64 = (GEO_COORD_MASK << GEO_COORD_BITS) | GEO_COORD_MASK;
const GEO_EARTH_RADIUS_METERS: f64 = 6_372_797.560_856;
const GEOHASH_ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
const GEOHASH_OUTPUT_LEN: usize = 11;
const STANDARD_GEOHASH_STEP: u32 = 26;
const STANDARD_GEOHASH_BITS: u32 = STANDARD_GEOHASH_STEP * 2;

#[derive(Default)]
struct GeoAddOptions {
    nx: bool,
    xx: bool,
    ch: bool,
}

#[derive(Clone)]
enum GeoSearchOrigin {
    FromMember(Vec<u8>),
    FromLonLat { longitude: f64, latitude: f64 },
}

#[derive(Clone, Copy)]
enum GeoSearchShape {
    ByRadius {
        radius_meters: f64,
        unit_to_meters: f64,
    },
    ByBox {
        width_meters: f64,
        height_meters: f64,
        unit_to_meters: f64,
    },
}

impl GeoSearchShape {
    fn unit_to_meters(self) -> f64 {
        match self {
            Self::ByRadius { unit_to_meters, .. } => unit_to_meters,
            Self::ByBox { unit_to_meters, .. } => unit_to_meters,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum GeoSortOrder {
    None,
    Asc,
    Desc,
}

#[derive(Clone)]
struct GeoSearchOptions {
    origin: GeoSearchOrigin,
    shape: GeoSearchShape,
    with_dist: bool,
    with_hash: bool,
    with_coord: bool,
    sort: GeoSortOrder,
    count: Option<usize>,
    any: bool,
    store_dist: bool,
}

#[derive(Clone)]
struct GeoSearchMatch {
    member: Vec<u8>,
    score: f64,
    longitude: f64,
    latitude: f64,
    distance_meters: f64,
}

struct GeoRadiusOptions {
    with_dist: bool,
    with_hash: bool,
    with_coord: bool,
    sort: GeoSortOrder,
    count: Option<usize>,
    any: bool,
    store_key: Option<Vec<u8>>,
    store_dist: bool,
}

impl Default for GeoRadiusOptions {
    fn default() -> Self {
        Self {
            with_dist: false,
            with_hash: false,
            with_coord: false,
            sort: GeoSortOrder::None,
            count: None,
            any: false,
            store_key: None,
            store_dist: false,
        }
    }
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
            parse_geo_unit_to_meters(unit)?
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

    pub(super) fn handle_geohash(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 3, "GEOHASH", GEOHASH_USAGE)?;

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
            let geohash = encode_standard_geohash(longitude, latitude);
            append_bulk_string(response_out, &geohash);
        }
        Ok(())
    }

    pub(super) fn handle_georadius(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_georadius_common(args, response_out, true, "GEORADIUS", GEORADIUS_USAGE)
    }

    pub(super) fn handle_georadius_ro(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_georadius_common(
            args,
            response_out,
            false,
            "GEORADIUS_RO",
            GEORADIUS_RO_USAGE,
        )
    }

    pub(super) fn handle_georadiusbymember(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_georadiusbymember_common(
            args,
            response_out,
            true,
            "GEORADIUSBYMEMBER",
            GEORADIUSBYMEMBER_USAGE,
        )
    }

    pub(super) fn handle_georadiusbymember_ro(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_georadiusbymember_common(
            args,
            response_out,
            false,
            "GEORADIUSBYMEMBER_RO",
            GEORADIUSBYMEMBER_RO_USAGE,
        )
    }

    fn handle_georadius_common(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        allow_store: bool,
        command_name: &'static str,
        usage: &'static str,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 6, command_name, usage)?;

        // SAFETY: caller guarantees argument backing memory validity.
        let source_key = unsafe { args[1].as_slice() };
        // SAFETY: caller guarantees argument backing memory validity.
        let longitude = parse_f64_ascii(unsafe { args[2].as_slice() })
            .ok_or(RequestExecutionError::ValueNotFloat)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let latitude = parse_f64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotFloat)?;
        if !geo_coordinates_in_range(longitude, latitude) {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let radius = parse_f64_ascii(unsafe { args[4].as_slice() })
            .ok_or(RequestExecutionError::ValueNotFloat)?;
        if radius < 0.0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let unit_to_meters = parse_geo_unit_to_meters(unsafe { args[5].as_slice() })?;
        let radius_options = parse_georadius_options(args, 6, allow_store)?;

        let query_options = GeoSearchOptions {
            origin: GeoSearchOrigin::FromLonLat {
                longitude,
                latitude,
            },
            shape: GeoSearchShape::ByRadius {
                radius_meters: radius * unit_to_meters,
                unit_to_meters,
            },
            with_dist: radius_options.with_dist,
            with_hash: radius_options.with_hash,
            with_coord: radius_options.with_coord,
            sort: radius_options.sort,
            count: radius_options.count,
            any: radius_options.any,
            store_dist: radius_options.store_dist,
        };
        execute_geo_query(
            self,
            source_key,
            &query_options,
            radius_options.store_key.as_deref(),
            response_out,
        )
    }

    fn handle_georadiusbymember_common(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
        allow_store: bool,
        command_name: &'static str,
        usage: &'static str,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 5, command_name, usage)?;

        // SAFETY: caller guarantees argument backing memory validity.
        let source_key = unsafe { args[1].as_slice() };
        // SAFETY: caller guarantees argument backing memory validity.
        let member = unsafe { args[2].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let radius = parse_f64_ascii(unsafe { args[3].as_slice() })
            .ok_or(RequestExecutionError::ValueNotFloat)?;
        if radius < 0.0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        // SAFETY: caller guarantees argument backing memory validity.
        let unit_to_meters = parse_geo_unit_to_meters(unsafe { args[4].as_slice() })?;
        let radius_options = parse_georadius_options(args, 5, allow_store)?;

        let query_options = GeoSearchOptions {
            origin: GeoSearchOrigin::FromMember(member),
            shape: GeoSearchShape::ByRadius {
                radius_meters: radius * unit_to_meters,
                unit_to_meters,
            },
            with_dist: radius_options.with_dist,
            with_hash: radius_options.with_hash,
            with_coord: radius_options.with_coord,
            sort: radius_options.sort,
            count: radius_options.count,
            any: radius_options.any,
            store_dist: radius_options.store_dist,
        };
        execute_geo_query(
            self,
            source_key,
            &query_options,
            radius_options.store_key.as_deref(),
            response_out,
        )
    }

    pub(super) fn handle_geosearch(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 7, "GEOSEARCH", GEOSEARCH_USAGE)?;
        let options = parse_geosearch_options(args, 2, true, false)?;
        // SAFETY: caller guarantees argument backing memory validity.
        let source_key = unsafe { args[1].as_slice() };
        execute_geo_query(self, source_key, &options, None, response_out)
    }

    pub(super) fn handle_geosearchstore(
        &self,
        args: &[ArgSlice],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 8, "GEOSEARCHSTORE", GEOSEARCHSTORE_USAGE)?;
        let options = parse_geosearch_options(args, 3, false, true)?;

        // SAFETY: caller guarantees argument backing memory validity.
        let destination_key = unsafe { args[1].as_slice() }.to_vec();
        // SAFETY: caller guarantees argument backing memory validity.
        let source_key = unsafe { args[2].as_slice() };
        execute_geo_query(
            self,
            source_key,
            &options,
            Some(destination_key.as_slice()),
            response_out,
        )
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

fn parse_geo_unit_to_meters(unit: &[u8]) -> Result<f64, RequestExecutionError> {
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

fn parse_geosearch_options(
    args: &[ArgSlice],
    mut index: usize,
    allow_reply_options: bool,
    allow_store_dist: bool,
) -> Result<GeoSearchOptions, RequestExecutionError> {
    let mut origin = None;
    let mut shape = None;
    let mut with_dist = false;
    let mut with_hash = false;
    let mut with_coord = false;
    let mut sort = GeoSortOrder::None;
    let mut count = None;
    let mut any = false;
    let mut store_dist = false;

    while index < args.len() {
        // SAFETY: caller guarantees argument backing memory validity.
        let token = unsafe { args[index].as_slice() };

        if ascii_eq_ignore_case(token, b"FROMMEMBER") {
            if origin.is_some() || index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let member = unsafe { args[index + 1].as_slice() }.to_vec();
            origin = Some(GeoSearchOrigin::FromMember(member));
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"FROMLONLAT") {
            if origin.is_some() || index + 2 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let longitude = parse_f64_ascii(unsafe { args[index + 1].as_slice() })
                .ok_or(RequestExecutionError::ValueNotFloat)?;
            // SAFETY: caller guarantees argument backing memory validity.
            let latitude = parse_f64_ascii(unsafe { args[index + 2].as_slice() })
                .ok_or(RequestExecutionError::ValueNotFloat)?;
            if !geo_coordinates_in_range(longitude, latitude) {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            origin = Some(GeoSearchOrigin::FromLonLat {
                longitude,
                latitude,
            });
            index += 3;
            continue;
        }
        if ascii_eq_ignore_case(token, b"BYRADIUS") {
            if shape.is_some() || index + 2 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let radius = parse_f64_ascii(unsafe { args[index + 1].as_slice() })
                .ok_or(RequestExecutionError::ValueNotFloat)?;
            if radius < 0.0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let unit_to_meters = parse_geo_unit_to_meters(unsafe { args[index + 2].as_slice() })?;
            shape = Some(GeoSearchShape::ByRadius {
                radius_meters: radius * unit_to_meters,
                unit_to_meters,
            });
            index += 3;
            continue;
        }
        if ascii_eq_ignore_case(token, b"BYBOX") {
            if shape.is_some() || index + 3 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let width = parse_f64_ascii(unsafe { args[index + 1].as_slice() })
                .ok_or(RequestExecutionError::ValueNotFloat)?;
            // SAFETY: caller guarantees argument backing memory validity.
            let height = parse_f64_ascii(unsafe { args[index + 2].as_slice() })
                .ok_or(RequestExecutionError::ValueNotFloat)?;
            if width < 0.0 || height < 0.0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let unit_to_meters = parse_geo_unit_to_meters(unsafe { args[index + 3].as_slice() })?;
            shape = Some(GeoSearchShape::ByBox {
                width_meters: width * unit_to_meters,
                height_meters: height * unit_to_meters,
                unit_to_meters,
            });
            index += 4;
            continue;
        }
        if allow_reply_options && ascii_eq_ignore_case(token, b"WITHDIST") {
            with_dist = true;
            index += 1;
            continue;
        }
        if allow_reply_options && ascii_eq_ignore_case(token, b"WITHHASH") {
            with_hash = true;
            index += 1;
            continue;
        }
        if allow_reply_options && ascii_eq_ignore_case(token, b"WITHCOORD") {
            with_coord = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"COUNT") {
            if count.is_some() || index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let raw_count = parse_i64_ascii(unsafe { args[index + 1].as_slice() })
                .ok_or(RequestExecutionError::ValueNotInteger)?;
            if raw_count <= 0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            count = Some(raw_count as usize);
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"ANY") {
            any = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"ASC") {
            sort = GeoSortOrder::Asc;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"DESC") {
            sort = GeoSortOrder::Desc;
            index += 1;
            continue;
        }
        if allow_store_dist && ascii_eq_ignore_case(token, b"STOREDIST") {
            store_dist = true;
            index += 1;
            continue;
        }
        return Err(RequestExecutionError::SyntaxError);
    }

    let origin = origin.ok_or(RequestExecutionError::SyntaxError)?;
    let shape = shape.ok_or(RequestExecutionError::SyntaxError)?;
    if any && count.is_none() {
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(GeoSearchOptions {
        origin,
        shape,
        with_dist,
        with_hash,
        with_coord,
        sort,
        count,
        any,
        store_dist,
    })
}

fn parse_georadius_options(
    args: &[ArgSlice],
    mut index: usize,
    allow_store: bool,
) -> Result<GeoRadiusOptions, RequestExecutionError> {
    let mut options = GeoRadiusOptions::default();
    while index < args.len() {
        // SAFETY: caller guarantees argument backing memory validity.
        let token = unsafe { args[index].as_slice() };
        if ascii_eq_ignore_case(token, b"WITHDIST") {
            options.with_dist = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"WITHHASH") {
            options.with_hash = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"WITHCOORD") {
            options.with_coord = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"COUNT") {
            if options.count.is_some() || index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            // SAFETY: caller guarantees argument backing memory validity.
            let raw_count = parse_i64_ascii(unsafe { args[index + 1].as_slice() })
                .ok_or(RequestExecutionError::ValueNotInteger)?;
            if raw_count <= 0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            options.count = Some(raw_count as usize);
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"ANY") {
            options.any = true;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"ASC") {
            options.sort = GeoSortOrder::Asc;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"DESC") {
            options.sort = GeoSortOrder::Desc;
            index += 1;
            continue;
        }
        if allow_store && ascii_eq_ignore_case(token, b"STORE") {
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            // SAFETY: caller guarantees argument backing memory validity.
            options.store_key = Some(unsafe { args[index + 1].as_slice() }.to_vec());
            options.store_dist = false;
            index += 2;
            continue;
        }
        if allow_store && ascii_eq_ignore_case(token, b"STOREDIST") {
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            // SAFETY: caller guarantees argument backing memory validity.
            options.store_key = Some(unsafe { args[index + 1].as_slice() }.to_vec());
            options.store_dist = true;
            index += 2;
            continue;
        }
        return Err(RequestExecutionError::SyntaxError);
    }

    if options.any && options.count.is_none() {
        return Err(RequestExecutionError::SyntaxError);
    }
    if options.store_key.is_some() && (options.with_dist || options.with_hash || options.with_coord)
    {
        return Err(RequestExecutionError::SyntaxError);
    }
    Ok(options)
}

fn execute_geo_query(
    processor: &RequestProcessor,
    source_key: &[u8],
    options: &GeoSearchOptions,
    store_key: Option<&[u8]>,
    response_out: &mut Vec<u8>,
) -> Result<(), RequestExecutionError> {
    let source_zset = match processor.load_zset_object(source_key)? {
        Some(entries) => entries,
        None => {
            if let Some(destination) = store_key {
                store_geosearch_result(processor, destination, &BTreeMap::new())?;
                append_integer(response_out, 0);
            } else {
                response_out.extend_from_slice(b"*0\r\n");
            }
            return Ok(());
        }
    };

    let (center_longitude, center_latitude) =
        resolve_geosearch_center(&source_zset, &options.origin)?;
    let mut matches = collect_geosearch_matches(
        &source_zset,
        center_longitude,
        center_latitude,
        options.shape,
    );
    apply_geosearch_sort_and_count(&mut matches, options);

    if let Some(destination) = store_key {
        let mut destination_zset = BTreeMap::new();
        let unit_to_meters = options.shape.unit_to_meters();
        for candidate in matches {
            let score = if options.store_dist {
                candidate.distance_meters / unit_to_meters
            } else {
                candidate.score
            };
            destination_zset.insert(candidate.member, score);
        }
        let stored = destination_zset.len() as i64;
        store_geosearch_result(processor, destination, &destination_zset)?;
        append_integer(response_out, stored);
        return Ok(());
    }

    append_geosearch_response(response_out, &matches, options);
    Ok(())
}

fn resolve_geosearch_center(
    zset: &BTreeMap<Vec<u8>, f64>,
    origin: &GeoSearchOrigin,
) -> Result<(f64, f64), RequestExecutionError> {
    match origin {
        GeoSearchOrigin::FromLonLat {
            longitude,
            latitude,
        } => Ok((*longitude, *latitude)),
        GeoSearchOrigin::FromMember(member) => {
            let Some(score) = zset.get(member).copied() else {
                return Err(RequestExecutionError::NoSuchKey);
            };
            let Some((longitude, latitude)) = decode_geo_score(score) else {
                return Err(RequestExecutionError::NoSuchKey);
            };
            Ok((longitude, latitude))
        }
    }
}

fn collect_geosearch_matches(
    zset: &BTreeMap<Vec<u8>, f64>,
    center_longitude: f64,
    center_latitude: f64,
    shape: GeoSearchShape,
) -> Vec<GeoSearchMatch> {
    let mut matches = Vec::new();
    for (member, score) in zset {
        let Some((longitude, latitude)) = decode_geo_score(*score) else {
            continue;
        };
        let distance_meters =
            geo_distance_meters(center_longitude, center_latitude, longitude, latitude);
        let in_shape = match shape {
            GeoSearchShape::ByRadius { radius_meters, .. } => distance_meters <= radius_meters,
            GeoSearchShape::ByBox {
                width_meters,
                height_meters,
                ..
            } => geo_box_contains(
                center_longitude,
                center_latitude,
                longitude,
                latitude,
                width_meters,
                height_meters,
            ),
        };
        if in_shape {
            matches.push(GeoSearchMatch {
                member: member.clone(),
                score: *score,
                longitude,
                latitude,
                distance_meters,
            });
        }
    }
    matches
}

fn apply_geosearch_sort_and_count(matches: &mut Vec<GeoSearchMatch>, options: &GeoSearchOptions) {
    let mut sort = options.sort;
    if options.count.is_some() && sort == GeoSortOrder::None && !options.any {
        sort = GeoSortOrder::Asc;
    }

    match sort {
        GeoSortOrder::None => {}
        GeoSortOrder::Asc => matches.sort_by(|left, right| {
            let distance_order = left
                .distance_meters
                .partial_cmp(&right.distance_meters)
                .unwrap_or(Ordering::Equal);
            if distance_order == Ordering::Equal {
                left.member.cmp(&right.member)
            } else {
                distance_order
            }
        }),
        GeoSortOrder::Desc => matches.sort_by(|left, right| {
            let distance_order = right
                .distance_meters
                .partial_cmp(&left.distance_meters)
                .unwrap_or(Ordering::Equal);
            if distance_order == Ordering::Equal {
                left.member.cmp(&right.member)
            } else {
                distance_order
            }
        }),
    }

    if let Some(count) = options.count {
        matches.truncate(count.min(matches.len()));
    }
}

fn append_geosearch_response(
    response_out: &mut Vec<u8>,
    matches: &[GeoSearchMatch],
    options: &GeoSearchOptions,
) {
    let option_count =
        options.with_dist as usize + options.with_hash as usize + options.with_coord as usize;
    response_out.extend_from_slice(format!("*{}\r\n", matches.len()).as_bytes());
    for entry in matches {
        if option_count == 0 {
            append_bulk_string(response_out, &entry.member);
            continue;
        }
        response_out.extend_from_slice(format!("*{}\r\n", option_count + 1).as_bytes());
        append_bulk_string(response_out, &entry.member);
        if options.with_dist {
            let distance_unit = entry.distance_meters / options.shape.unit_to_meters();
            let text = format_geo_number(distance_unit, 4);
            append_bulk_string(response_out, text.as_bytes());
        }
        if options.with_hash {
            append_integer(response_out, entry.score as i64);
        }
        if options.with_coord {
            response_out.extend_from_slice(b"*2\r\n");
            let longitude_text = format_geo_coordinate(entry.longitude);
            let latitude_text = format_geo_coordinate(entry.latitude);
            append_bulk_string(response_out, longitude_text.as_bytes());
            append_bulk_string(response_out, latitude_text.as_bytes());
        }
    }
}

fn geo_box_contains(
    center_longitude: f64,
    center_latitude: f64,
    longitude: f64,
    latitude: f64,
    width_meters: f64,
    height_meters: f64,
) -> bool {
    let half_height_delta = ((height_meters * 0.5) / GEO_EARTH_RADIUS_METERS).to_degrees();
    if (latitude - center_latitude).abs() > half_height_delta {
        return false;
    }

    let cos_lat = center_latitude.to_radians().cos().abs();
    let half_width_delta = if cos_lat <= f64::EPSILON {
        GEO_LONGITUDE_MAX
    } else {
        (((width_meters * 0.5) / (GEO_EARTH_RADIUS_METERS * cos_lat)).to_degrees())
            .min(GEO_LONGITUDE_MAX)
    };
    let longitude_delta = normalized_longitude_delta(longitude - center_longitude).abs();
    longitude_delta <= half_width_delta
}

fn normalized_longitude_delta(delta: f64) -> f64 {
    let mut normalized = delta % 360.0;
    if normalized > 180.0 {
        normalized -= 360.0;
    }
    if normalized < -180.0 {
        normalized += 360.0;
    }
    normalized
}

fn store_geosearch_result(
    processor: &RequestProcessor,
    destination: &[u8],
    result_zset: &BTreeMap<Vec<u8>, f64>,
) -> Result<(), RequestExecutionError> {
    processor.expire_key_if_needed(destination)?;
    let destination_had_string = processor.key_exists(destination)?;
    let string_deleted = if destination_had_string {
        delete_string_value_for_geo_store_overwrite(processor, destination)?
    } else {
        false
    };

    if result_zset.is_empty() {
        let object_deleted = processor.object_delete(destination)?;
        if string_deleted && !object_deleted {
            processor.bump_watch_version(destination);
        }
        return Ok(());
    }
    processor.save_zset_object(destination, result_zset)
}

fn delete_string_value_for_geo_store_overwrite(
    processor: &RequestProcessor,
    key: &[u8],
) -> Result<bool, RequestExecutionError> {
    let key_vec = key.to_vec();
    let mut store = processor.lock_string_store_for_key(key);
    let mut session = store.session(&processor.functions);
    let mut info = DeleteInfo::default();
    let status = session
        .delete(&key_vec, &mut info)
        .map_err(map_delete_error)?;
    match status {
        DeleteOperationStatus::TombstonedInPlace | DeleteOperationStatus::AppendedTombstone => {
            processor.remove_string_key_metadata(key);
            Ok(true)
        }
        DeleteOperationStatus::NotFound => {
            processor.remove_string_key_metadata(key);
            Ok(false)
        }
        DeleteOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
    }
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

fn encode_standard_geohash(longitude: f64, latitude: f64) -> [u8; GEOHASH_OUTPUT_LEN] {
    let mut output = [b'0'; GEOHASH_OUTPUT_LEN];
    let lat_bits = quantize_standard_geohash_coordinate(
        latitude,
        STANDARD_GEOHASH_LAT_MIN,
        STANDARD_GEOHASH_LAT_MAX,
    );
    let lon_bits =
        quantize_standard_geohash_coordinate(longitude, GEO_LONGITUDE_MIN, GEO_LONGITUDE_MAX);
    let hash_bits = interleave64(lat_bits, lon_bits);

    for (index, slot) in output.iter_mut().enumerate() {
        let alphabet_index = if index == GEOHASH_OUTPUT_LEN - 1 {
            // Redis/Valkey expose 11 chars for compatibility, even though only 52 bits are encoded.
            0usize
        } else {
            let shift = STANDARD_GEOHASH_BITS - ((index as u32 + 1) * 5);
            ((hash_bits >> shift) & 0x1f) as usize
        };
        *slot = GEOHASH_ALPHABET[alphabet_index];
    }

    output
}

fn quantize_standard_geohash_coordinate(value: f64, min: f64, max: f64) -> u32 {
    let normalized = ((value - min) / (max - min)).clamp(0.0, 1.0);
    (normalized * (1u64 << STANDARD_GEOHASH_STEP) as f64) as u32
}

fn interleave64(xlo: u32, ylo: u32) -> u64 {
    const MASKS: [u64; 5] = [
        0x5555_5555_5555_5555,
        0x3333_3333_3333_3333,
        0x0f0f_0f0f_0f0f_0f0f,
        0x00ff_00ff_00ff_00ff,
        0x0000_ffff_0000_ffff,
    ];
    const SHIFTS: [u32; 5] = [1, 2, 4, 8, 16];

    let mut x = xlo as u64;
    let mut y = ylo as u64;
    for (mask, shift) in MASKS.iter().zip(SHIFTS.iter()).rev() {
        x = (x | (x << shift)) & mask;
        y = (y | (y << shift)) & mask;
    }
    x | (y << 1)
}
