# Deep Research Instructions: JVM Performance Gap for Garnet-like Systems

## Context

We are evaluating the feasibility of reimplementing Microsoft Garnet (a high-performance Redis-compatible cache store written in C#) in Kotlin/JVM. Garnet achieves ~100x Redis performance through C#-specific features: generic monomorphization (reified generics with struct constraints), value types (structs with explicit memory layout), unsafe pointer arithmetic, and cache-line aligned data structures.

The core question is: **How much of the C#/Rust vs JVM performance gap can be closed by JVM platform improvements?**

We need up-to-date (as of February 2026) information on two topics.

---

## Research Topic 1: Project Valhalla Current Status

### Questions to Answer

1. **Release Status**: Has any part of Project Valhalla shipped as GA (non-preview) in any JDK release (JDK 23, 24, 25, or later)? If still in preview, which JDK version has the latest preview, and what is the expected GA timeline?

2. **Value Classes (JEP 401)**:
   - What is the final/current specification for value classes?
   - Do value class arrays **guarantee** flat (inline) layout, or can the JVM choose to use indirection?
   - Can value classes specify field alignment or layout order (equivalent to C#'s `StructLayout(Explicit)` / `FieldOffset`)?
   - What is the maximum size of a value class that will be flattened in arrays?
   - Can value classes be used with `VarHandle` for atomic CAS operations on individual fields?

3. **Universal Generics / Specialized Generics**:
   - What is the current status of generic specialization for value types in Valhalla?
   - Does `List<value class MyStruct>` produce a specialized (monomorphized) implementation, or is it still erased to `List<Object>` with just layout flattening?
   - Is there any form of code specialization (different compiled code per type parameter), or only data layout specialization?

4. **Performance Benchmarks**:
   - Are there any published benchmarks comparing Valhalla value classes vs regular Java objects for:
     - Array iteration throughput
     - Hash table lookup with value-type keys
     - Cache-miss rates for flat vs indirected arrays
   - Any benchmarks from real-world projects that adopted Valhalla early?

5. **Ecosystem Readiness**:
   - Have major libraries (Netty, Guava, Eclipse Collections, Chronicle) adopted or announced plans to adopt value classes?
   - Does Kotlin have support for Valhalla value classes? If not, what is the timeline?

### Search Queries to Try
- "Project Valhalla JDK 25 GA release value classes 2025 2026"
- "JEP 401 value classes status 2026"
- "Valhalla universal generics specialization status"
- "Valhalla value class array flattening benchmark performance"
- "Kotlin Valhalla value class support"
- "JEP 401 value objects final specification"
- site:openjdk.org "valhalla" 2025 OR 2026
- site:mail.openjdk.org "valhalla" value class

---

## Research Topic 2: GraalVM Native Image and Generic Specialization

### Questions to Answer

1. **Generic Handling in Native Image**:
   - Does GraalVM Native Image (as of 2025-2026) perform any form of generic specialization during AOT compilation?
   - Since Native Image uses closed-world analysis and knows all type instantiations at build time, does it generate specialized code for different generic type arguments?
   - If not, is this on the GraalVM roadmap?

2. **Unsafe/Panama Support**:
   - What is the current status of `sun.misc.Unsafe` support in GraalVM Native Image? Are there still restrictions?
   - Does GraalVM Native Image support Project Panama's `MemorySegment` and `Foreign Function Interface`?

3. **Performance Data**:
   - Any benchmarks comparing GraalVM Native Image vs JVM (C2 JIT) for:
     - Low-latency / high-throughput workloads similar to cache stores
     - Generic-heavy code paths
   - Does GraalVM's PGO (Profile-Guided Optimization) help with generic specialization?

4. **GraalVM JIT (as C2 replacement)**:
   - What is the latest performance comparison between GraalVM JIT and C2 for server workloads?
   - Has GraalVM JIT improved its inlining depth limits or escape analysis since 2024?

### Search Queries to Try
- "GraalVM native image generic specialization monomorphization 2025 2026"
- "GraalVM native image sun.misc.Unsafe support status"
- "GraalVM native image Panama MemorySegment support"
- "GraalVM vs C2 JIT performance benchmark 2025 2026"
- "GraalVM native image PGO profile guided optimization generics"
- site:graalvm.org "generic" OR "specialization" 2025 OR 2026

---

## Desired Output Format

For each topic, please provide:

1. **Factual Status** (with sources/links): What is confirmed as of the latest available information?
2. **Timeline Assessment**: When is GA expected? What is the confidence level?
3. **Performance Impact Estimate**: Based on available benchmarks or expert analysis, what is the realistic performance improvement for a Garnet-like workload?
4. **Implications for Garnet Reimplementation**: Does this change the recommendation of "Rust as primary language"? Under what conditions would Kotlin/JVM become viable for the storage engine layer?

Please cite specific JEP numbers, mailing list posts, conference talks, or blog posts from OpenJDK/Oracle/GraalVM team members where possible.
