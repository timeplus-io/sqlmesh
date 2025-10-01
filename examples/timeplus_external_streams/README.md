# SQLMesh + Timeplus + ClickHouse Integration Example

This example demonstrates **multi-gateway orchestration** with SQLMesh managing both Timeplus (streaming database) and ClickHouse (analytical database) through declarative SQL models.

## What This Demonstrates

✅ **Multi-Gateway Orchestration**: Single SQLMesh project managing multiple database systems
✅ **Cross-Gateway Dependencies**: Timeplus models referencing ClickHouse tables via external tables
✅ **Declarative Pipeline**: Infrastructure and data flow defined entirely in SQL
✅ **Real-Time Streaming**: Kafka → Timeplus streaming → ClickHouse analytics
✅ **Unified Deployment**: All models deployed with `sqlmesh plan --auto-apply`

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Git

## Installation

### Step 1: Install timeplus-sqlmesh

```bash
# Create a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install timeplus-sqlmesh from PyPI
pip install timeplus-sqlmesh

# Install Kafka library for data generation
pip install kafka-python
```

### Step 2: Verify Installation

```bash
# Verify installation
sqlmesh --version

# Verify Timeplus adapter is available
python -c "from sqlmesh.core.engine_adapter import TimeplusEngineAdapter; print('✓ Timeplus adapter loaded')"
```

### Step 3: Get the Example Files

```bash
# Clone the repository to get examples
git clone https://github.com/timeplus-io/sqlmesh.git
cd sqlmesh/examples/timeplus_external_streams
```

## Quick Start

After installation, navigate to the example directory (from Step 3 above):

```bash
cd sqlmesh/examples/timeplus_external_streams
```

### Option 1: Automated Demo Script

```bash
./demo.sh
```

This script will:
1. Clean up any previous runs
2. Start Docker containers (Kafka, Timeplus, ClickHouse)
3. Deploy all SQLMesh models
4. Generate test data
5. Verify end-to-end pipeline

**Note**: Ensure your virtual environment with `timeplus-sqlmesh` installed is activated before running.

### Option 2: Manual Step-by-Step

```bash
# Ensure you're in the example directory
cd examples/timeplus_external_streams

# 1. Start infrastructure
docker compose up -d

# Wait ~10 seconds for services to be ready
sleep 10

# 2. Deploy all models (timeplus-sqlmesh must be installed)
sqlmesh plan --auto-apply

# 3. Generate test data
python scripts/generate_data.py --duration 10 --rate 60

# 4. Verify results in ClickHouse
docker exec e2e_clickhouse clickhouse-client --query \
  "SELECT * FROM default.e2e_aggregation_results ORDER BY win_start DESC LIMIT 10"
```

## Architecture (Simplified)

```
┌─────────────┐
│   Kafka     │  Raw events (e2e_events topic)
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│                Timeplus (Gateway: local)                    │
│                                                             │
│  1. kafka_events_stream (External Stream)                  │
│         ↓                                                   │
│  2. direct_to_clickhouse_mv (Materialized View)            │
│         • 5-second tumbling windows                        │
│         • Direct aggregation: Kafka → ClickHouse           │
│         ↓                                                   │
│  3. clickhouse_results (External Table) ←┐                 │
│         • References ClickHouse table     │                 │
│         • cross-gateway dependency ───────┘                 │
└─────────────────────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│             ClickHouse (Gateway: clickhouse_local)          │
│                                                             │
│  0. e2e_aggregation_results (MergeTree)                    │
│         • Created by SQLMesh via clickhouse_local gateway   │
│         • ORDER BY (user_id, win_start)                     │
└─────────────────────────────────────────────────────────────┘
```

**Key Innovation**: Direct aggregation from Kafka to ClickHouse in a single materialized view, eliminating intermediate streams.

## File Structure

```
├── models/                              # SQLMesh models (4 files)
│   ├── 00_clickhouse_table.sql         # ClickHouse table (clickhouse_local gateway)
│   ├── 01_kafka_stream.sql             # Kafka external stream
│   ├── 02_clickhouse_external.sql      # ClickHouse external table reference
│   └── 03_direct_aggregation_mv.sql    # Direct MV: Kafka → ClickHouse
├── scripts/
│   └── generate_data.py                # Kafka event generator
├── config.yaml                          # SQLMesh configuration (multi-gateway)
├── docker-compose.yml                   # All services (Kafka, Timeplus, ClickHouse)
├── demo.sh                              # Automated demo script
└── README.md                            # This file
```

## Key Features

### 1. Multi-Gateway Configuration

```yaml
# config.yaml
gateways:
  local:              # Timeplus gateway
    connection:
      type: timeplus
      port: 3218

  clickhouse_local:   # ClickHouse gateway
    connection:
      type: clickhouse
      port: 8124
```

### 2. Cross-Gateway Dependencies

```sql
-- Model 00: ClickHouse table (clickhouse_local gateway)
MODEL (
    name default.e2e_aggregation_results,
    gateway clickhouse_local,
    storage_format 'MergeTree',
    ...
)

-- Model 04: Timeplus external table (local gateway)
MODEL (
    name default.clickhouse_results,
    gateway local,
    depends_on [default.e2e_aggregation_results],  -- Cross-gateway dependency!
    physical_properties (
        external_type = 'clickhouse',
        database = 'default',
        table = 'e2e_aggregation_results'
    ),
    ...
)
```

### 3. Correct Deployment Order

SQLMesh automatically determines the correct order based on `depends_on`:

1. **ClickHouse table** (`e2e_aggregation_results`) - clickhouse_local gateway
2. **Kafka stream** (`kafka_events_stream`) - Timeplus
3. **ClickHouse external table** (`clickhouse_results`) - Timeplus ← depends on #1
4. **Aggregation MV** (`direct_to_clickhouse_mv`) - Timeplus ← depends on #3

## Requirements

- Docker & Docker Compose
- Python 3.9+
- timeplus-sqlmesh (`pip install timeplus-sqlmesh`)
- kafka-python (for data generation)

## Deployment Commands

```bash
# Deploy all models
sqlmesh plan --auto-apply

# View model DAG
sqlmesh dag

# Run tests (if defined)
sqlmesh test

# Cleanup and redeploy
rm sqlmesh_state.db
sqlmesh plan --auto-apply
```

## Verification

```bash
# Check ClickHouse table
docker exec e2e_clickhouse clickhouse-client --query \
    "SELECT count() FROM default.e2e_aggregation_results"

# Check Timeplus objects
docker exec e2e_timeplus timeplusd-client --query \
    "SELECT name, engine FROM system.tables WHERE database = 'default'"

# Generate test data (600 events in 10 seconds)
python scripts/generate_data.py --duration 10 --rate 60

# View results
docker exec e2e_clickhouse clickhouse-client --query \
    "SELECT * FROM default.e2e_aggregation_results ORDER BY win_start DESC LIMIT 10" \
    --format=Pretty
```

## Cleanup

```bash
# Stop and remove everything
docker compose down -v

# Remove SQLMesh state
rm sqlmesh_state.db

# Clean Python cache
rm -rf models/__pycache__
```

## Troubleshooting

### "Stream doesn't exist" error
The demo script includes automatic retry logic. If running manually, just run `sqlmesh plan --auto-apply` again. This is a timing issue that resolves on retry.

### No data in ClickHouse
1. Check Kafka has events:
   ```bash
   docker exec e2e_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic e2e_events --from-beginning --max-messages 5
   ```
2. Check Timeplus materialized view:
   ```bash
   docker exec e2e_timeplus timeplusd-client --query "SELECT count() FROM default.direct_to_clickhouse_mv"
   ```
3. Wait longer (10-15 seconds) for data to flow through pipeline

### Port conflicts
If ports 3218 (Timeplus), 8124 (ClickHouse), or 9092 (Kafka) are in use:
- Check with: `netstat -tuln | grep -E '3218|8124|9092'`
- Edit `docker-compose.yml` to use different ports

## Why SQLMesh?

Traditional approach requires manual orchestration of multiple tools and systems. **With timeplus-sqlmesh, deploy everything with one command:**

```bash
sqlmesh plan --auto-apply
```

SQLMesh acts as a **unified orchestration layer** managing infrastructure across heterogeneous systems (Kafka, Timeplus, ClickHouse) through pure declarative SQL models!

## Learn More

- **[PyPI Package](https://pypi.org/project/timeplus-sqlmesh/)** - Install with `pip install timeplus-sqlmesh`
- **[Timeplus Documentation](https://docs.timeplus.com)** - Timeplus streaming SQL guide
- **[Timeplus Adapter Reference](../../docs/integrations/engines/timeplus.md)** - Full integration documentation
- **[SQLMesh Documentation](https://sqlmesh.readthedocs.io)** - Core SQLMesh concepts
- **[GitHub Repository](https://github.com/timeplus-io/sqlmesh)** - Source code and examples

## About timeplus-sqlmesh

**timeplus-sqlmesh** is SQLMesh with native support for [Timeplus](https://www.timeplus.com), a high-performance streaming database. This package enables:

✅ **Seamless Timeplus Integration**: Native support for streams, materialized views, and external tables
✅ **Multi-Gateway Orchestration**: Manage Timeplus alongside ClickHouse, PostgreSQL, Snowflake, and more
✅ **Streaming SQL Pipelines**: Kafka → Timeplus → Analytics databases with declarative models
✅ **Production-Ready**: Full test coverage and CI/CD integration

**Installation**: `pip install timeplus-sqlmesh`
**Package**: https://pypi.org/project/timeplus-sqlmesh/

## Experimentation Ideas

This example is designed to be extended. Try:
- Modify aggregation logic in `models/03_direct_aggregation_mv.sql`
- Change window sizes (currently 5 seconds)
- Add more Timeplus transformations or external integrations
- Connect additional data sources (PostgreSQL, MySQL, etc.)
- Scale to multiple Kafka partitions

## License

This example is part of the SQLMesh project and follows the same license.
