use criterion::{Criterion, black_box, criterion_group, criterion_main};
use garnet_common::{ArgSlice, parse_resp_command_arg_slices};
use garnet_server::RequestProcessor;

fn benchmark_set_and_get(c: &mut Criterion) {
    let processor = RequestProcessor::new().unwrap();
    let mut set_args = [ArgSlice::EMPTY; 8];
    let mut get_args = [ArgSlice::EMPTY; 8];
    let set_frame = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
    let get_frame = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";

    let set_meta = parse_resp_command_arg_slices(set_frame, &mut set_args).unwrap();
    let get_meta = parse_resp_command_arg_slices(get_frame, &mut get_args).unwrap();

    let mut setup_response = Vec::new();
    processor
        .execute(&set_args[..set_meta.argument_count], &mut setup_response)
        .unwrap();

    c.bench_function("request_processor_set", |b| {
        let mut response = Vec::with_capacity(32);
        b.iter(|| {
            response.clear();
            processor
                .execute(&set_args[..set_meta.argument_count], &mut response)
                .unwrap();
            black_box(&response);
        });
    });

    c.bench_function("request_processor_get", |b| {
        let mut response = Vec::with_capacity(64);
        b.iter(|| {
            response.clear();
            processor
                .execute(&get_args[..get_meta.argument_count], &mut response)
                .unwrap();
            black_box(&response);
        });
    });
}

criterion_group!(benches, benchmark_set_and_get);
criterion_main!(benches);
