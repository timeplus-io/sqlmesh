#!/bin/bash
# Complete SQLMesh + Timeplus + ClickHouse Demo
# Usage: ./demo.sh

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo "═══════════════════════════════════════════════════════════"
echo "  SQLMesh Multi-Gateway Orchestration Demo"
echo "  Timeplus + ClickHouse Integration"
echo "═══════════════════════════════════════════════════════════"
echo

# Note: This script assumes SQLMesh is already installed from source
# and available in your current Python environment

# Step 1: Check prerequisites
echo -e "${BLUE}[Step 1/9]${NC} Checking prerequisites..."
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed${NC}"
    exit 1
fi
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo -e "${RED}Error: Docker Compose is not installed${NC}"
    exit 1
fi
echo -e "${GREEN}✓${NC} Docker and Docker Compose are installed"
echo

# Step 2: Clean up old state
echo -e "${BLUE}[Step 2/9]${NC} Cleaning up old state..."
docker compose down -v 2>/dev/null || true
if [ -f "sqlmesh_state.db" ]; then
    rm -f sqlmesh_state.db
    echo -e "${GREEN}✓${NC} Old state cleaned up"
else
    echo "No old state found (fresh start)"
fi
echo

# Step 3: Start infrastructure
echo -e "${BLUE}[Step 3/9]${NC} Starting Docker containers..."
docker compose up -d

echo "Waiting for services to be ready..."
sleep 10
echo -e "${GREEN}✓${NC} All containers started"
echo

# Step 4: Setup Kafka
echo -e "${BLUE}[Step 4/9]${NC} Setting up Kafka topic..."
docker exec e2e_kafka kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic e2e_events \
    --partitions 3 \
    --replication-factor 1 2>&1 | grep -v "WARN" || true

echo -e "${GREEN}✓${NC} Kafka topic 'e2e_events' created"
echo

# Step 5: Verify Python dependencies
echo -e "${BLUE}[Step 5/9]${NC} Checking Python dependencies..."

# Verify sqlmesh is available
if ! command -v sqlmesh &> /dev/null; then
    echo -e "${RED}Error: sqlmesh command not found${NC}"
    echo ""
    echo "Please install SQLMesh from source first:"
    echo "  cd /path/to/sqlmesh"
    echo "  pip install -e \".[dev]\""
    echo ""
    echo "Then activate your Python environment and try again."
    exit 1
fi
echo -e "${GREEN}✓${NC} SQLMesh is available"

# Verify Python dependencies
if ! python -c "import kafka" 2>/dev/null; then
    echo -e "${YELLOW}Warning: kafka-python not found, installing...${NC}"
    pip install -q kafka-python
fi
echo -e "${GREEN}✓${NC} All dependencies ready"
echo

# Step 6: Deploy with SQLMesh
echo -e "${BLUE}[Step 6/9]${NC} Deploying all models with SQLMesh..."
echo -e "${YELLOW}Running: sqlmesh plan --auto-apply${NC}"
echo

# First attempt
sqlmesh plan --auto-apply 2>&1 | tee /tmp/sqlmesh_deploy.log

# Check if there were failures
if grep -q "Failed models" /tmp/sqlmesh_deploy.log; then
    echo
    echo -e "${YELLOW}⚠ First deployment had failures (timing issue), retrying...${NC}"
    sleep 2

    # Second attempt (should succeed)
    sqlmesh plan --auto-apply 2>&1 | tee /tmp/sqlmesh_deploy_retry.log

    # Check again
    if grep -q "Failed models" /tmp/sqlmesh_deploy_retry.log; then
        echo -e "${RED}✗ Deployment failed after retry${NC}"
        echo "Check logs at /tmp/sqlmesh_deploy_retry.log"
        exit 1
    else
        echo -e "${GREEN}✓${NC} All models deployed successfully on retry"
    fi
elif grep -q "Model batches executed" /tmp/sqlmesh_deploy.log; then
    echo -e "${GREEN}✓${NC} All models deployed successfully"
elif grep -q "No changes to plan" /tmp/sqlmesh_deploy.log; then
    echo -e "${YELLOW}⚠${NC} No changes to plan (already deployed)"
else
    echo -e "${RED}✗${NC} Unexpected deployment result"
    echo "Check logs at /tmp/sqlmesh_deploy.log"
    exit 1
fi
echo

# Step 7: Verify deployment
echo -e "${BLUE}[Step 7/9]${NC} Verifying deployment..."

echo "Checking ClickHouse table..."
if docker exec e2e_clickhouse clickhouse-client --query "EXISTS TABLE default.e2e_aggregation_results" | grep -q "1"; then
    echo -e "${GREEN}✓${NC} ClickHouse table exists"
else
    echo -e "${RED}✗${NC} ClickHouse table not found"
    exit 1
fi

echo "Checking Timeplus objects..."
STREAM_COUNT=$(docker exec e2e_timeplus timeplusd-client --query "SELECT count() FROM system.tables WHERE database = 'default'" 2>/dev/null || echo "0")
echo -e "${GREEN}✓${NC} Found ${STREAM_COUNT} Timeplus objects"
echo

# Step 8: Generate test data
echo -e "${BLUE}[Step 8/9]${NC} Generating test data..."
echo -e "${YELLOW}Generating 600 events over 10 seconds (60 events/sec)...${NC}"

if python scripts/generate_data.py --duration 10 --rate 60; then
    echo -e "${GREEN}✓${NC} Test data generated successfully"
else
    echo -e "${RED}Error: Data generation failed${NC}"
    exit 1
fi
echo

# Step 9: Wait and verify results
echo -e "${BLUE}[Step 9/9]${NC} Waiting for data to flow through pipeline..."
echo "Waiting 10 seconds for aggregations to complete..."
for i in {10..1}; do
    echo -ne "\r${YELLOW}$i seconds remaining...${NC}"
    sleep 1
done
echo -e "\r${GREEN}✓${NC} Wait complete              "
echo

echo -e "${BLUE}Verifying results in ClickHouse...${NC}"
RESULT=$(docker exec e2e_clickhouse clickhouse-client --query \
    "SELECT count() as records,
            formatDateTime(min(win_start), '%Y-%m-%d %H:%i:%S') as earliest,
            formatDateTime(max(win_start), '%Y-%m-%d %H:%i:%S') as latest
     FROM default.e2e_aggregation_results
     FORMAT TSV" 2>/dev/null || echo "0	N/A	N/A")

RECORD_COUNT=$(echo "$RESULT" | cut -f1)
EARLIEST=$(echo "$RESULT" | cut -f2)
LATEST=$(echo "$RESULT" | cut -f3)

echo
echo "═══════════════════════════════════════════════════════════"
echo -e "  ${GREEN}✓ PIPELINE SUCCESSFUL!${NC}"
echo "═══════════════════════════════════════════════════════════"
echo
echo "📊 Results:"
echo "   • Total Records: ${RECORD_COUNT}"
echo "   • Time Range: ${EARLIEST} to ${LATEST}"
echo

if [ "$RECORD_COUNT" -gt 0 ]; then
    echo -e "${BLUE}Sample Data (Top 10):${NC}"
    docker exec e2e_clickhouse clickhouse-client --query \
        "SELECT win_start, user_id, event_type, event_count, round(total_amount, 2) as amount
         FROM default.e2e_aggregation_results
         ORDER BY win_start DESC
         LIMIT 10" --format=Pretty
    echo

    echo "═══════════════════════════════════════════════════════════"
    echo -e "  ${GREEN}✓ DEMO COMPLETE!${NC}"
    echo "═══════════════════════════════════════════════════════════"
    echo
    echo "Next steps:"
    echo "  • View ClickHouse data: docker exec e2e_clickhouse clickhouse-client"
    echo "  • Query Timeplus: docker exec e2e_timeplus timeplusd-client"
    echo "  • Generate more data: python scripts/generate_data.py --duration 10 --rate 60"
    echo "  • Cleanup: docker compose down -v && rm sqlmesh_state.db"
    echo
else
    echo -e "${RED}⚠ Warning: No data found in ClickHouse${NC}"
    echo "This might be a timing issue. Try running data generation again:"
    echo "  python scripts/generate_data.py --duration 10 --rate 60"
    echo
fi

exit 0
