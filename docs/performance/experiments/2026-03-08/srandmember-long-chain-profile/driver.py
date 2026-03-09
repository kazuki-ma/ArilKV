#!/usr/bin/env python3
import argparse
import socket
import time


class RespClient:
    def __init__(self, host: str, port: int):
        self.sock = socket.create_connection((host, port))
        self.reader = self.sock.makefile("rb", buffering=0)

    def close(self):
        try:
            self.reader.close()
        finally:
            self.sock.close()

    def write_command(self, *parts: bytes):
        frame = [f"*{len(parts)}\r\n".encode()]
        for part in parts:
            frame.append(f"${len(part)}\r\n".encode())
            frame.append(part)
            frame.append(b"\r\n")
        self.sock.sendall(b"".join(frame))

    def command(self, *parts: bytes):
        self.write_command(*parts)
        return self.read_value()

    def read_line(self) -> bytes:
        line = self.reader.readline()
        if not line:
            raise RuntimeError("unexpected EOF")
        if not line.endswith(b"\r\n"):
            raise RuntimeError(f"invalid RESP line: {line!r}")
        return line[:-2]

    def read_value(self):
        line = self.read_line()
        prefix = line[:1]
        payload = line[1:]
        if prefix == b"+":
            return payload
        if prefix == b":":
            return int(payload)
        if prefix == b"$":
            length = int(payload)
            if length < 0:
                return None
            data = self.reader.read(length + 2)
            if data is None or len(data) != length + 2 or not data.endswith(b"\r\n"):
                raise RuntimeError("invalid bulk payload")
            return data[:-2]
        if prefix == b"*":
            length = int(payload)
            if length < 0:
                return None
            return [self.read_value() for _ in range(length)]
        raise RuntimeError(f"unsupported RESP type: {line!r}")


def wait_for_bgsave_to_finish(client: RespClient, timeout_seconds: float):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        payload = client.command(b"INFO")
        if not isinstance(payload, bytes):
            raise RuntimeError("INFO did not return bulk payload")
        for line in payload.decode().split("\r\n"):
            if line.startswith("rdb_bgsave_in_progress:") and line.endswith("0"):
                return
        time.sleep(0.01)
    raise RuntimeError("timed out waiting for BGSAVE to finish")


def create_set_like_redis_external_test(client: RespClient, key: bytes, member_count: int):
    client.command(b"DEL", key)
    for index in range(member_count):
        added = client.command(b"SADD", key, f"m:{index}".encode())
        if added != 1:
            raise RuntimeError(f"SADD returned {added} for m:{index}")


def rem_hash_set_top_n_like_redis_external_test(
    client: RespClient,
    key: bytes,
    remove_count: int,
):
    cursor = 0
    members_to_remove = []
    while True:
        response = client.command(b"SSCAN", key, str(cursor).encode())
        if not isinstance(response, list) or len(response) != 2:
            raise RuntimeError(f"unexpected SSCAN response: {response!r}")
        cursor = int(response[0])
        members = response[1]
        if not isinstance(members, list):
            raise RuntimeError(f"unexpected SSCAN members: {members!r}")
        for member in members:
            members_to_remove.append(member)
            if len(members_to_remove) >= remove_count:
                break
        if len(members_to_remove) >= remove_count or cursor == 0:
            break

    if len(members_to_remove) != remove_count:
        raise RuntimeError(
            f"expected {remove_count} members to remove, got {len(members_to_remove)}"
        )

    for member in members_to_remove:
        client.write_command(b"SREM", key, member)
    for _ in members_to_remove:
        removed = client.read_value()
        if removed != 1:
            raise RuntimeError(f"SREM returned {removed!r}")


def chi_square_value(samples: list[bytes], population: set[bytes]) -> float:
    if not samples or not population:
        return 0.0
    expected = len(samples) / len(population)
    counts: dict[bytes, int] = {}
    for sample in samples:
        counts[sample] = counts.get(sample, 0) + 1
    total = 0.0
    for member in population:
        observed = counts.get(member, 0)
        delta = observed - expected
        total += (delta * delta) / expected
    return total


def run_scenario(host: str, port: int, rdb_key_save_delay_micros: int):
    client = RespClient(host, port)
    try:
        print("phase=config", flush=True)
        assert client.command(b"CONFIG", b"SET", b"save", b"") == b"OK"
        assert (
            client.command(b"CONFIG", b"SET", b"set-max-listpack-entries", b"0") == b"OK"
        )
        assert (
            client.command(
                b"CONFIG",
                b"SET",
                b"rdb-key-save-delay",
                str(rdb_key_save_delay_micros).encode(),
            )
            == b"OK"
        )

        started = time.time()
        print("phase=create_set", flush=True)
        create_set_like_redis_external_test(client, b"myset", 100_000)
        after_create = time.time()

        print("phase=warmup", flush=True)
        warmup = client.command(b"SRANDMEMBER", b"myset", b"100")
        if not isinstance(warmup, list) or len(warmup) != 100:
            raise RuntimeError("unexpected warmup SRANDMEMBER response")

        print("phase=trim_to_500", flush=True)
        assert client.command(b"BGSAVE") == b"Background saving started"
        rem_hash_set_top_n_like_redis_external_test(client, b"myset", 100_000 - 500)
        assert client.command(b"SCARD", b"myset") == 500

        print("phase=wait_bgsave_1", flush=True)
        wait_for_bgsave_to_finish(client, 10.0)

        print("phase=spop", flush=True)
        popped = client.command(b"SPOP", b"myset", b"1")
        if not isinstance(popped, list) or len(popped) != 1:
            raise RuntimeError("unexpected SPOP response")

        print("phase=wait_bgsave_2", flush=True)
        assert client.command(b"BGSAVE") == b"Background saving started"
        if client.command(b"SRANDMEMBER", b"myset") is None:
            raise RuntimeError("unexpected null SRANDMEMBER result")
        wait_for_bgsave_to_finish(client, 10.0)

        print("phase=coverage_loop", flush=True)
        expected_members = client.command(b"SMEMBERS", b"myset")
        if not isinstance(expected_members, list):
            raise RuntimeError("unexpected SMEMBERS response")
        expected_members = set(expected_members)

        observed_members: set[bytes] = set()
        iterations = 1000
        while iterations > 0:
            iterations -= 1
            sample = client.command(b"SRANDMEMBER", b"myset", b"-10")
            if not isinstance(sample, list):
                raise RuntimeError("unexpected SRANDMEMBER -10 response")
            observed_members.update(sample)
            if observed_members == expected_members:
                break
        if iterations == 0:
            raise RuntimeError("SRANDMEMBER -10 did not cover all remaining members")

        print("phase=trim_to_30", flush=True)
        rem_hash_set_top_n_like_redis_external_test(client, b"myset", 499 - 30)
        assert client.command(b"SCARD", b"myset") == 30

        print("phase=histogram", flush=True)
        remaining = client.command(b"SMEMBERS", b"myset")
        if not isinstance(remaining, list):
            raise RuntimeError("unexpected SMEMBERS response after trim")
        remaining = set(remaining)

        histogram_samples: list[bytes] = []
        for _ in range(1000):
            sample = client.command(b"SRANDMEMBER", b"myset", b"10")
            if not isinstance(sample, list) or len(sample) != 10:
                raise RuntimeError("unexpected SRANDMEMBER 10 response")
            histogram_samples.extend(sample)

        chi_square = chi_square_value(histogram_samples, remaining)
        if chi_square >= 73.0:
            raise RuntimeError(f"chi-square too high: {chi_square}")

        finished = time.time()
        print(
            f"create_seconds={after_create - started:.3f} total_seconds={finished - started:.3f} "
            f"chi_square={chi_square:.3f} remaining={len(remaining)}"
        )
    finally:
        client.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument(
        "--rdb-key-save-delay-micros",
        type=int,
        default=2147483647,
    )
    args = parser.parse_args()
    run_scenario(args.host, args.port, args.rdb_key_save_delay_micros)


if __name__ == "__main__":
    main()
