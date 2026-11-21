# Shopping Sessions

The root Dockerfile builds the Linux x86-64 entry point. The pipeline reads the
scorer's DATA_DIR and writes the two Parquet tables to OUTPUT_DIR. NCPU sets
the maximum parallelism. It has no package dependencies at run time.

The pipeline memory-maps CSV input and reads it in parallel, assigns linked IDs
with union-find, and uses separate partitions for event deduplication and
per-person sessions. A sliding click window gives each session one credit and
one assist per eligible campaign. The output writer emits Parquet directly.

## Local checks

Extract the fixture and generator archives supplied by the organizers, then run
these commands from the repository root. The comparison scripts require Polars.

```sh
python3 -m pip install polars
unzip starter-duckdb.zip
make fixtures FIXTURES=starter-duckdb/fixtures
unzip generator.zip
cargo build --release --manifest-path generator/Cargo.toml
generator/target/release/generate --seed 1 --events 1000000 --clicks 250000 --output dev
unzip dev-answer.zip
make run-dev
make check-dev
```

With .NET 10 installed, the same entry point can also be built and tested
without Docker.

```sh
dotnet build -c Release
python3 scripts/run_fixtures.py starter-duckdb/fixtures -- dotnet bin/Release/net10.0/shopping-sessions.dll
```
