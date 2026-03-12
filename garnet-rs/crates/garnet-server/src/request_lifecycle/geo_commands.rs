use super::*;
use std::cmp::Ordering;

const GEOADD_USAGE: &str =
    "GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]";
const GEOPOS_USAGE: &str = "GEOPOS key member [member ...]";
const GEODIST_USAGE: &str = "GEODIST key member1 member2 [M|KM|FT|MI]";
const GEOHASH_USAGE: &str = "GEOHASH key member [member ...]";
const GEORADIUS_USAGE: &str = "GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]";
const GEORADIUSBYMEMBER_USAGE: &str = "GEORADIUSBYMEMBER key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]";
const GEORADIUS_RO_USAGE: &str = "GEORADIUS_RO key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC]";
const GEORADIUSBYMEMBER_RO_USAGE: &str = "GEORADIUSBYMEMBER_RO key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC]";
const GEOSEARCH_USAGE: &str = "GEOSEARCH key FROMMEMBER member|FROMLONLAT lon lat BYRADIUS radius unit|BYBOX width height unit [options]";
const GEOSEARCHSTORE_USAGE: &str = "GEOSEARCHSTORE destination source FROMMEMBER member|FROMLONLAT lon lat BYRADIUS radius unit|BYBOX width height unit [options]";
const GEO_LONGITUDE_MIN: f64 = -180.0;
const GEO_LONGITUDE_MAX: f64 = 180.0;
const GEO_LATITUDE_MIN: f64 = -85.051_128_78;
const GEO_LATITUDE_MAX: f64 = 85.051_128_78;
const STANDARD_GEOHASH_LAT_MIN: f64 = -90.0;
const STANDARD_GEOHASH_LAT_MAX: f64 = 90.0;
const GEO_COORD_BITS: u32 = 26;
const GEO_COORD_MASK: u64 = (1u64 << GEO_COORD_BITS) - 1;
const GEO_SCORE_MAX: u64 = (GEO_COORD_MASK << GEO_COORD_BITS) | GEO_COORD_MASK;
const GEO_SCORE_SCALE: f64 = (1u64 << GEO_COORD_BITS) as f64;
const GEO_EARTH_RADIUS_METERS: f64 = 6_372_797.560_856;
const GEO_DISTANCE_EPSILON: f64 = 1e-15;
const GEOHASH_ALPHABET: &[u8; 32] = b"0123456789bcdefghjkmnpqrstuvwxyz";
const GEOHASH_OUTPUT_LEN: usize = 11;
const STANDARD_GEOHASH_STEP: u32 = 26;
const STANDARD_GEOHASH_BITS: u32 = STANDARD_GEOHASH_STEP * 2;

#[derive(Default)]
struct GeoAddOptions {
    mode: GeoAddMode,
    ch: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum GeoAddMode {
    #[default]
    Any,
    Nx,
    Xx,
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 5, "GEOADD", GEOADD_USAGE)?;

        let key = RedisKey::from(args[1]);
        let parsed_options = parse_geoadd_options(args, 2)?;
        let options = parsed_options.options;
        let mut index = parsed_options.next_index;

        let remaining = args.len().saturating_sub(index);
        if remaining == 0 {
            return Err(RequestExecutionError::WrongArity {
                command: "GEOADD",
                expected: GEOADD_USAGE,
            });
        }
        if remaining % 3 != 0 {
            if parse_f64_ascii(args[index]).is_none() {
                return Err(RequestExecutionError::SyntaxError);
            }
            return Err(RequestExecutionError::WrongArity {
                command: "GEOADD",
                expected: GEOADD_USAGE,
            });
        }

        let mut zset = self
            .load_zset_object(DbKeyRef::new(current_request_selected_db(), &key))?
            .unwrap_or_default();
        let mut updated = false;
        let mut added = 0i64;
        let mut changed = 0i64;

        while index + 2 < args.len() {
            let longitude =
                parse_f64_ascii(args[index]).ok_or(RequestExecutionError::ValueNotFloat)?;
            let latitude =
                parse_f64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotFloat)?;
            if !geo_coordinates_in_range(longitude, latitude) {
                return Err(RequestExecutionError::ValueOutOfRange);
            }

            let member = args[index + 2];
            let previous = zset.get(member).copied();

            match options.mode {
                GeoAddMode::Nx if previous.is_some() => {
                    index += 3;
                    continue;
                }
                GeoAddMode::Xx if previous.is_none() => {
                    index += 3;
                    continue;
                }
                _ => {}
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
            self.save_zset_object(DbKeyRef::new(current_request_selected_db(), &key), &zset)?;
        }

        let result = if options.ch { changed } else { added };
        append_integer(response_out, result);
        Ok(())
    }

    pub(super) fn handle_geopos(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "GEOPOS", GEOPOS_USAGE)?;

        let key = args[1];
        let zset = self.load_zset_object(DbKeyRef::new(current_request_selected_db(), key))?;
        let resp3 = self.resp_protocol_version().is_resp3();

        append_array_length(response_out, args.len() - 2);
        for member in &args[2..] {
            let Some(score) = zset.as_ref().and_then(|entries| entries.get(*member)) else {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_array(response_out);
                }
                continue;
            };
            let Some(decoded_score) = decode_geo_score(*score) else {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_array(response_out);
                }
                continue;
            };
            let longitude = decoded_score.longitude;
            let latitude = decoded_score.latitude;

            append_array_length(response_out, 2);
            if resp3 {
                append_double(response_out, longitude);
                append_double(response_out, latitude);
            } else {
                let longitude_text = format_geo_coordinate(longitude);
                let latitude_text = format_geo_coordinate(latitude);
                append_bulk_string(response_out, longitude_text.as_bytes());
                append_bulk_string(response_out, latitude_text.as_bytes());
            }
        }
        Ok(())
    }

    pub(super) fn handle_geodist(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_ranged_arity(args, 4, 5, "GEODIST", GEODIST_USAGE)?;

        let unit_to_meters = if args.len() == 5 {
            let unit = args[4];
            parse_geo_unit_to_meters(unit)?
        } else {
            1.0
        };

        let key = args[1];
        let resp3 = self.resp_protocol_version().is_resp3();
        let zset = match self.load_zset_object(DbKeyRef::new(current_request_selected_db(), key))? {
            Some(entries) => entries,
            None => {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                return Ok(());
            }
        };

        let member_a = args[2];
        let member_b = args[3];
        let Some(score_a) = zset.get(member_a).copied() else {
            if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        };
        let Some(score_b) = zset.get(member_b).copied() else {
            if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        };

        let Some(decoded_score_a) = decode_geo_score(score_a) else {
            if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        };
        let Some(decoded_score_b) = decode_geo_score(score_b) else {
            if resp3 {
                append_null(response_out);
            } else {
                append_null_bulk_string(response_out);
            }
            return Ok(());
        };
        let lon_a = decoded_score_a.longitude;
        let lat_a = decoded_score_a.latitude;
        let lon_b = decoded_score_b.longitude;
        let lat_b = decoded_score_b.latitude;

        let meters = geo_distance_meters(lon_a, lat_a, lon_b, lat_b);
        let unit_value = meters / unit_to_meters;
        if resp3 {
            append_double(response_out, unit_value);
        } else {
            let text = format_geo_distance(unit_value);
            append_bulk_string(response_out, text.as_bytes());
        }
        Ok(())
    }

    pub(super) fn handle_geohash(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 2, "GEOHASH", GEOHASH_USAGE)?;

        let key = args[1];
        let zset = self.load_zset_object(DbKeyRef::new(current_request_selected_db(), key))?;
        let resp3 = self.resp_protocol_version().is_resp3();

        append_array_length(response_out, args.len() - 2);
        for member in &args[2..] {
            let Some(score) = zset.as_ref().and_then(|entries| entries.get(*member)) else {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                continue;
            };
            let Some(decoded_score) = decode_geo_score(*score) else {
                if resp3 {
                    append_null(response_out);
                } else {
                    append_null_bulk_string(response_out);
                }
                continue;
            };
            let longitude = decoded_score.longitude;
            let latitude = decoded_score.latitude;
            let geohash = encode_standard_geohash(longitude, latitude);
            append_bulk_string(response_out, &geohash);
        }
        Ok(())
    }

    pub(super) fn handle_georadius(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        self.handle_georadius_common(args, response_out, true, "GEORADIUS", GEORADIUS_USAGE)
    }

    pub(super) fn handle_georadius_ro(
        &self,
        args: &[&[u8]],
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
        args: &[&[u8]],
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
        args: &[&[u8]],
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
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        allow_store: bool,
        command_name: &'static str,
        usage: &'static str,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 6, command_name, usage)?;

        let source_key = args[1];
        let longitude = parse_f64_ascii(args[2]).ok_or(RequestExecutionError::ValueNotFloat)?;
        let latitude = parse_f64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotFloat)?;
        if !geo_coordinates_in_range(longitude, latitude) {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let radius = parse_f64_ascii(args[4]).ok_or(RequestExecutionError::ValueNotFloat)?;
        if radius < 0.0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let unit_to_meters = parse_geo_unit_to_meters(args[5])?;
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
        let resp3 = self.resp_protocol_version().is_resp3();
        execute_geo_query(
            self,
            source_key,
            &query_options,
            radius_options.store_key.as_deref(),
            response_out,
            resp3,
        )
    }

    fn handle_georadiusbymember_common(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
        allow_store: bool,
        command_name: &'static str,
        usage: &'static str,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 5, command_name, usage)?;

        let source_key = args[1];
        let member = args[2].to_vec();
        let radius = parse_f64_ascii(args[3]).ok_or(RequestExecutionError::ValueNotFloat)?;
        if radius < 0.0 {
            return Err(RequestExecutionError::ValueOutOfRange);
        }
        let unit_to_meters = parse_geo_unit_to_meters(args[4])?;
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
        let resp3 = self.resp_protocol_version().is_resp3();
        execute_geo_query(
            self,
            source_key,
            &query_options,
            radius_options.store_key.as_deref(),
            response_out,
            resp3,
        )
    }

    pub(super) fn handle_geosearch(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 7, "GEOSEARCH", GEOSEARCH_USAGE)?;
        let options = parse_geosearch_options(args, 2, true, false)?;
        let source_key = args[1];
        let resp3 = self.resp_protocol_version().is_resp3();
        execute_geo_query(self, source_key, &options, None, response_out, resp3)
    }

    pub(super) fn handle_geosearchstore(
        &self,
        args: &[&[u8]],
        response_out: &mut Vec<u8>,
    ) -> Result<(), RequestExecutionError> {
        ensure_min_arity(args, 8, "GEOSEARCHSTORE", GEOSEARCHSTORE_USAGE)?;
        let options = parse_geosearch_options(args, 3, false, true)?;

        let destination_key = RedisKey::from(args[1]);
        let source_key = args[2];
        let resp3 = self.resp_protocol_version().is_resp3();
        execute_geo_query(
            self,
            source_key,
            &options,
            Some(destination_key.as_slice()),
            response_out,
            resp3,
        )
    }
}

fn parse_geoadd_options(
    args: &[&[u8]],
    mut index: usize,
) -> Result<ParsedGeoAddOptions, RequestExecutionError> {
    let mut options = GeoAddOptions::default();
    while index < args.len() {
        let token = args[index];
        if ascii_eq_ignore_case(token, b"NX") {
            if options.mode == GeoAddMode::Xx {
                return Err(RequestExecutionError::SyntaxError);
            }
            options.mode = GeoAddMode::Nx;
            index += 1;
            continue;
        }
        if ascii_eq_ignore_case(token, b"XX") {
            if options.mode == GeoAddMode::Nx {
                return Err(RequestExecutionError::SyntaxError);
            }
            options.mode = GeoAddMode::Xx;
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
    Ok(ParsedGeoAddOptions {
        options,
        next_index: index,
    })
}

struct ParsedGeoAddOptions {
    options: GeoAddOptions,
    next_index: usize,
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
    args: &[&[u8]],
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
        let token = args[index];

        if ascii_eq_ignore_case(token, b"FROMMEMBER") {
            if origin.is_some() || index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let member = args[index + 1].to_vec();
            origin = Some(GeoSearchOrigin::FromMember(member));
            index += 2;
            continue;
        }
        if ascii_eq_ignore_case(token, b"FROMLONLAT") {
            if origin.is_some() || index + 2 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            let longitude =
                parse_f64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotFloat)?;
            let latitude =
                parse_f64_ascii(args[index + 2]).ok_or(RequestExecutionError::ValueNotFloat)?;
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
            let radius =
                parse_f64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotFloat)?;
            if radius < 0.0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            let unit_to_meters = parse_geo_unit_to_meters(args[index + 2])?;
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
            let width =
                parse_f64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotFloat)?;
            let height =
                parse_f64_ascii(args[index + 2]).ok_or(RequestExecutionError::ValueNotFloat)?;
            if width < 0.0 || height < 0.0 {
                return Err(RequestExecutionError::ValueOutOfRange);
            }
            let unit_to_meters = parse_geo_unit_to_meters(args[index + 3])?;
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
            let raw_count =
                parse_i64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
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

    let origin = origin.ok_or(RequestExecutionError::GeoSearchRequiresOrigin)?;
    let shape = shape.ok_or(RequestExecutionError::GeoSearchRequiresShape)?;
    if any && count.is_none() {
        return Err(RequestExecutionError::GeoAnyRequiresCount);
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
    args: &[&[u8]],
    mut index: usize,
    allow_store: bool,
) -> Result<GeoRadiusOptions, RequestExecutionError> {
    let mut options = GeoRadiusOptions::default();
    while index < args.len() {
        let token = args[index];
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
            let raw_count =
                parse_i64_ascii(args[index + 1]).ok_or(RequestExecutionError::ValueNotInteger)?;
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
            options.store_key = Some(args[index + 1].to_vec());
            options.store_dist = false;
            index += 2;
            continue;
        }
        if allow_store && ascii_eq_ignore_case(token, b"STOREDIST") {
            if index + 1 >= args.len() {
                return Err(RequestExecutionError::SyntaxError);
            }
            options.store_key = Some(args[index + 1].to_vec());
            options.store_dist = true;
            index += 2;
            continue;
        }
        return Err(RequestExecutionError::SyntaxError);
    }

    if options.any && options.count.is_none() {
        return Err(RequestExecutionError::GeoAnyRequiresCount);
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
    resp3: bool,
) -> Result<(), RequestExecutionError> {
    let source_zset = match processor
        .load_zset_object(DbKeyRef::new(current_request_selected_db(), source_key))?
    {
        Some(entries) => entries,
        None => {
            if let Some(destination) = store_key {
                store_geosearch_result(processor, destination, &BTreeMap::new())?;
                append_integer(response_out, 0);
            } else {
                append_array_length(response_out, 0);
            }
            return Ok(());
        }
    };

    let center = resolve_geosearch_center(&source_zset, &options.origin)?;
    let mut matches = collect_geosearch_matches(
        &source_zset,
        center.longitude,
        center.latitude,
        options.shape,
    );
    if options.sort == GeoSortOrder::None
        && let GeoSearchOrigin::FromMember(origin_member) = &options.origin
    {
        move_match_member_to_front(&mut matches, origin_member);
    }
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

    append_geosearch_response(response_out, &matches, options, resp3);
    Ok(())
}

fn resolve_geosearch_center(
    zset: &BTreeMap<Vec<u8>, f64>,
    origin: &GeoSearchOrigin,
) -> Result<GeoCoordinates, RequestExecutionError> {
    match origin {
        GeoSearchOrigin::FromLonLat {
            longitude,
            latitude,
        } => Ok(GeoCoordinates {
            longitude: *longitude,
            latitude: *latitude,
        }),
        GeoSearchOrigin::FromMember(member) => {
            let Some(score) = zset.get(member).copied() else {
                return Err(RequestExecutionError::NoSuchKey);
            };
            let Some(decoded_score) = decode_geo_score(score) else {
                return Err(RequestExecutionError::NoSuchKey);
            };
            let longitude = decoded_score.longitude;
            let latitude = decoded_score.latitude;
            Ok(GeoCoordinates {
                longitude,
                latitude,
            })
        }
    }
}

struct GeoCoordinates {
    longitude: f64,
    latitude: f64,
}

fn collect_geosearch_matches(
    zset: &BTreeMap<Vec<u8>, f64>,
    center_longitude: f64,
    center_latitude: f64,
    shape: GeoSearchShape,
) -> Vec<GeoSearchMatch> {
    let mut matches = Vec::new();
    let mut entries: Vec<(&Vec<u8>, f64)> = zset
        .iter()
        .map(|(member, score)| (member, *score))
        .collect();
    entries.sort_by(|(left_member, left_score), (right_member, right_score)| {
        let score_order = left_score
            .partial_cmp(right_score)
            .unwrap_or(Ordering::Equal);
        if score_order == Ordering::Equal {
            left_member.cmp(right_member)
        } else {
            score_order
        }
    });

    for (member, score) in entries {
        let Some(decoded_score) = decode_geo_score(score) else {
            continue;
        };
        let longitude = decoded_score.longitude;
        let latitude = decoded_score.latitude;
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
                score,
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

    if options.any
        && let Some(count) = options.count
    {
        matches.truncate(count.min(matches.len()));
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

    if !options.any
        && let Some(count) = options.count
    {
        matches.truncate(count.min(matches.len()));
    }
}

fn move_match_member_to_front(matches: &mut Vec<GeoSearchMatch>, member: &[u8]) {
    let Some(index) = matches
        .iter()
        .position(|entry| entry.member.as_slice() == member)
    else {
        return;
    };
    if index == 0 {
        return;
    }
    let center = matches.remove(index);
    matches.insert(0, center);
}

fn append_geosearch_response(
    response_out: &mut Vec<u8>,
    matches: &[GeoSearchMatch],
    options: &GeoSearchOptions,
    resp3: bool,
) {
    let option_count =
        options.with_dist as usize + options.with_hash as usize + options.with_coord as usize;
    append_array_length(response_out, matches.len());
    for entry in matches {
        if option_count == 0 {
            append_bulk_string(response_out, &entry.member);
            continue;
        }
        append_array_length(response_out, option_count + 1);
        append_bulk_string(response_out, &entry.member);
        if options.with_dist {
            let distance_unit = entry.distance_meters / options.shape.unit_to_meters();
            let text = format_geo_distance(distance_unit);
            append_bulk_string(response_out, text.as_bytes());
        }
        if options.with_hash {
            append_integer(response_out, entry.score as i64);
        }
        if options.with_coord {
            append_array_length(response_out, 2);
            if resp3 {
                append_double(response_out, entry.longitude);
                append_double(response_out, entry.latitude);
            } else {
                let longitude_text = format_geo_coordinate(entry.longitude);
                let latitude_text = format_geo_coordinate(entry.latitude);
                append_bulk_string(response_out, longitude_text.as_bytes());
                append_bulk_string(response_out, latitude_text.as_bytes());
            }
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
    let lat_distance = geo_lat_distance_meters(latitude, center_latitude);
    if lat_distance > height_meters * 0.5 {
        return false;
    }

    let lon_distance = geo_distance_meters(longitude, latitude, center_longitude, latitude);
    lon_distance <= width_meters * 0.5
}

fn store_geosearch_result(
    processor: &RequestProcessor,
    destination: &[u8],
    result_zset: &BTreeMap<Vec<u8>, f64>,
) -> Result<(), RequestExecutionError> {
    processor.expire_key_if_needed(DbKeyRef::new(current_request_selected_db(), destination))?;
    let (destination_had_string, destination_object_type) = processor
        .key_type_snapshot_for_setkey_overwrite(DbKeyRef::new(
            current_request_selected_db(),
            destination,
        ))?;
    let string_deleted = if destination_had_string {
        delete_string_value_for_geo_store_overwrite(processor, destination)?
    } else {
        false
    };

    if result_zset.is_empty() {
        let object_deleted =
            processor.object_delete(DbKeyRef::new(current_request_selected_db(), destination))?;
        if string_deleted && !object_deleted {
            processor.bump_watch_version(DbKeyRef::new(current_request_selected_db(), destination));
        }
        return Ok(());
    }
    processor.save_zset_object(
        DbKeyRef::new(current_request_selected_db(), destination),
        result_zset,
    )?;
    processor.notify_setkey_overwrite_events(
        current_request_selected_db(),
        destination,
        destination_had_string,
        destination_object_type,
        Some(ObjectTypeTag::Zset),
    );
    Ok(())
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
            processor.remove_string_key_metadata(DbKeyRef::new(current_request_selected_db(), key));
            Ok(true)
        }
        DeleteOperationStatus::NotFound => {
            processor.remove_string_key_metadata(DbKeyRef::new(current_request_selected_db(), key));
            Ok(false)
        }
        DeleteOperationStatus::RetryLater => Err(RequestExecutionError::StorageBusy),
    }
}

fn encode_geo_score(longitude: f64, latitude: f64) -> f64 {
    let lon_bits = encode_geo_component(longitude, GEO_LONGITUDE_MIN, GEO_LONGITUDE_MAX);
    let lat_bits = encode_geo_component(latitude, GEO_LATITUDE_MIN, GEO_LATITUDE_MAX);
    let packed = interleave64(lat_bits, lon_bits) & GEO_SCORE_MAX;
    packed as f64
}

struct DecodedGeoScore {
    longitude: f64,
    latitude: f64,
}

fn decode_geo_score(score: f64) -> Option<DecodedGeoScore> {
    if !score.is_finite() || score < 0.0 || score > GEO_SCORE_MAX as f64 {
        return None;
    }
    if score.fract() != 0.0 {
        return None;
    }
    let packed = score as u64;
    let separated = deinterleave64(packed);
    let lat_bits = (separated & 0xffff_ffff) as u32;
    let lon_bits = (separated >> 32) as u32;
    let longitude = decode_geo_component(lon_bits, GEO_LONGITUDE_MIN, GEO_LONGITUDE_MAX);
    let latitude = decode_geo_component(lat_bits, GEO_LATITUDE_MIN, GEO_LATITUDE_MAX);
    Some(DecodedGeoScore {
        longitude,
        latitude,
    })
}

fn encode_geo_component(value: f64, min: f64, max: f64) -> u32 {
    let normalized = ((value - min) / (max - min)).clamp(0.0, 1.0);
    let scaled = normalized * GEO_SCORE_SCALE;
    scaled as u32
}

fn decode_geo_component(value: u32, min: f64, max: f64) -> f64 {
    let scale = max - min;
    let min_coord = min + (value as f64 / GEO_SCORE_SCALE) * scale;
    let max_coord = min + ((value as f64 + 1.0) / GEO_SCORE_SCALE) * scale;
    ((min_coord + max_coord) * 0.5).clamp(min, max)
}

fn geo_lat_distance_meters(lat_a: f64, lat_b: f64) -> f64 {
    GEO_EARTH_RADIUS_METERS * (lat_b.to_radians() - lat_a.to_radians()).abs()
}

fn geo_distance_meters(lon_a: f64, lat_a: f64, lon_b: f64, lat_b: f64) -> f64 {
    let lon_a_radians = lon_a.to_radians();
    let lon_b_radians = lon_b.to_radians();
    let lon_sine = ((lon_b_radians - lon_a_radians) * 0.5).sin();
    if lon_sine.abs() <= GEO_DISTANCE_EPSILON {
        return geo_lat_distance_meters(lat_a, lat_b);
    }

    let lat_a_radians = lat_a.to_radians();
    let lat_b_radians = lat_b.to_radians();
    let lat_sine = ((lat_b_radians - lat_a_radians) * 0.5).sin();
    let a = lat_sine * lat_sine + lat_a_radians.cos() * lat_b_radians.cos() * lon_sine * lon_sine;
    2.0 * GEO_EARTH_RADIUS_METERS * a.sqrt().asin()
}

fn format_geo_coordinate(value: f64) -> String {
    format_geo_number(value, 17)
}

fn format_geo_distance(value: f64) -> String {
    format!("{value:.4}")
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

fn deinterleave64(interleaved: u64) -> u64 {
    const MASKS: [u64; 6] = [
        0x5555_5555_5555_5555,
        0x3333_3333_3333_3333,
        0x0f0f_0f0f_0f0f_0f0f,
        0x00ff_00ff_00ff_00ff,
        0x0000_ffff_0000_ffff,
        0x0000_0000_ffff_ffff,
    ];
    const SHIFTS: [u32; 6] = [0, 1, 2, 4, 8, 16];

    let mut x = interleaved;
    let mut y = interleaved >> 1;
    for (mask, shift) in MASKS.iter().zip(SHIFTS.iter()) {
        x = (x | (x >> *shift)) & *mask;
        y = (y | (y >> *shift)) & *mask;
    }
    x | (y << 32)
}
