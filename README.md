# AItriagedRegression
AI Powere Regression triaged - AWS S3, Athena, Glue, MCP Server, Amazon KIRO
# Karate Test Triage Pipeline

AI-powered test failure triage system that ingests Karate test reports into AWS Athena for automated analysis via an MCP server.

## Architecture

```
Karate Tests → JSON Report → Flattener (CSV) → S3 (JSONL) → Glue Crawler → Athena → MCP Server → AI Agent (Kiro)
```

## Prerequisites

### Local Environment

| Requirement | Version | Purpose |
|---|---|---|
| Java | 25 | Karate test execution |
| Maven | 3.x | Build and test runner |
| Python | 3.9+ | Ingestion pipeline and MCP server |
| AWS CLI | 2.x | AWS authentication and configuration |
| pip packages | `boto3`, `pandas`, `mcp` | Python dependencies |

Install `uv` (Python package manager, required if running MCP servers via `uvx`):

```bash
# macOS (Homebrew)
brew install uv

# Or via pip
pip install uv
```

See the [uv installation guide](https://docs.astral.sh/uv/getting-started/installation/) for other platforms.

Install Python dependencies:

```bash
pip install boto3 pandas mcp
```

### AWS Resources

The following AWS resources must be created before running the pipeline.

---

#### 1. S3 Bucket

Create the S3 bucket used for storing raw logs and Athena query results.

- Bucket name: `gaurav-logs-archive`
- Region: Match your Athena/Glue region

Required folder structure (created automatically on first upload, but can be pre-created):

```
gaurav-logs-archive/
├── raw_logs/                  # JSONL test result files land here
└── athena-results/            # Athena query output location
```

```bash
aws s3 mb s3://gaurav-logs-archive --region us-east-1
```

---

#### 2. IAM User / Role

Create an IAM user or role with the following permissions. Attach these as a custom policy.

```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "S3Tables",
			"Effect": "Allow",
			"Action": "s3tables:*",
			"Resource": [
				"arn:aws:s3tables:us-east-1:893130088***:bucket/*",
				"arn:aws:s3tables:us-east-1:893130088***:bucket/poc-logs",
				"arn:aws:s3tables:us-east-1:893130088***:bucket/poc-logs/*"
			]
		},
		{
			"Sid": "Athena",
			"Effect": "Allow",
			"Action": [
				"athena:StartQueryExecution",
				"athena:GetQueryExecution",
				"athena:GetQueryResults",
				"athena:StopQueryExecution",
				"athena:GetWorkGroup"
			],
			"Resource": "*"
		},
		{
			"Sid": "GlueCatalog",
			"Effect": "Allow",
			"Action": [
				"glue:GetDatabase",
				"glue:GetDatabases",
				"glue:GetTable",
				"glue:GetTables",
				"glue:GetPartitions"
			],
			"Resource": [
				"arn:aws:glue:us-east-1:893130088***:catalog",
				"arn:aws:glue:us-east-1:893130088***:database/*",
				"arn:aws:glue:us-east-1:893130088***:table/*/*"
			]
		},
		{
			"Sid": "S3AthenaResults",
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:PutObject",
				"s3:ListBucket",
				"s3:GetBucketLocation"
			],
			"Resource": [
				"arn:aws:s3:::aws-athena-query-results-893130088***-us-east-1",
				"arn:aws:s3:::aws-athena-query-results-893130088***-us-east-1/*",
				"arn:aws:s3:::aws-athena-query-results-*",
				"arn:aws:s3:::aws-athena-query-results-*/*"
			]
		},
		{
			"Sid": "S3DataAccess",
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:ListBucket",
				"s3:GetBucketLocation"
			],
			"Resource": [
				"arn:aws:s3:::poc-logs-karate-*",
				"arn:aws:s3:::poc-logs-karate-*/*"
			]
		},
		{
			"Sid": "S3AthenaOutputBucket",
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:PutObject",
				"s3:ListBucket",
				"s3:GetBucketLocation",
				"s3:CreateBucket"
			],
			"Resource": [
				"arn:aws:s3:::gaurav-logs-archive",
				"arn:aws:s3:::gaurav-logs-archive/*"
			]
		}
	]
}
```

Configure AWS credentials locally:

```bash
aws configure
# AWS Access Key ID: [your-access-key]
# AWS Secret Access Key: [your-secret-key]
# Default region name: us-east-1
# Default output format: json
```

---

#### 3. Athena Database

Create the Athena database that the MCP server queries against.

```sql
CREATE DATABASE IF NOT EXISTS karate_triage_db;
```

Run via AWS Console (Athena Query Editor) or CLI:

```bash
aws athena start-query-execution \
  --query-string "CREATE DATABASE IF NOT EXISTS karate_triage_db" \
  --result-configuration OutputLocation=s3://gaurav-logs-archive/athena-results/
```

---

#### 4. Athena Table
##### Drop table
```sql
DROP TABLE IF EXISTS karate_triage_db.raw_logs;

```
##### Create the `raw_logs` table pointing to the S3 raw logs folder.
```sql
CREATE EXTERNAL TABLE karate_triage_db.raw_logs (
    batch_id string,
    execution_time double,
    scenario_name string,
    environment string,
    http_method string,
    url string,
    request_body string,
    response_body string,
    status string
)
PARTITIONED BY (
    year string,
    month string,
    day string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'ignore.malformed.json' = 'true',
    'case.insensitive' = 'true'
)
LOCATION 's3://gaurav-logs-archive/raw_logs/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.year.type' = 'integer',
    'projection.year.range' = '2025,2030',
    'projection.month.type' = 'integer',
    'projection.month.range' = '01,12',
    'projection.month.digits' = '2',
    'projection.day.type' = 'integer',
    'projection.day.range' = '01,31',
    'projection.day.digits' = '2',
    'storage.location.template' = 's3://gaurav-logs-archive/raw_logs/year=${year}/month=${month}/day=${day}/'
);

##### Alter the `raw_logs` table.
```sql
ALTER TABLE karate_triage_db.raw_logs
ADD COLUMNS (request_headers string);
```
---

#### 5. AWS Glue Crawler (Optional)

If you prefer auto-discovery of schema changes, create a Glue crawler:

- Crawler name: `karate-logs-crawler`
- Data source: `s3://gaurav-logs-archive/raw_logs/`
- Target database: `karate_triage_db`
- Table prefix: (leave empty or use `karate_`)
- Schedule: On-demand or after each ingestion run

---

## Pipeline Components

### 1. Karate Test Suite (`src/test/java/net/gaurav/`)

Karate BDD test scenarios that run against `https://jsonplaceholder.typicode.com`. Includes both passing and intentionally failing scenarios for triage training.

- `poc-log-generator1.feature` — Scenarios 01–06 (schema, list, nested, boolean, drift, contains)
- `poc-log-generator2.feature` — Scenarios 07–13 (boundary, header, partial, empty, POST, DELETE)

Run tests via the runner class:

```bash
# Run in default environment (uat)
mvn clean test -Dtest=net.gaurav.runner.PocLogGeneratorRunner

# Run in a specific environment
mvn clean test -Dtest=net.gaurav.runner.PocLogGeneratorRunner -Dkarate.env=dev

# Run a specific feature file directly
mvn test -Dkarate.options="classpath:net/gaurav/poc-log-generator1.feature" -Dtest=net.gaurav.runner.PocLogGeneratorRunner -Dkarate.env=dev
```

Reports are generated at: `target/karate-reports/*.karate-json.txt`

### 2. Report Flattener (`util/karate_report_flattener.py`)

Parses Karate JSON reports and flattens each scenario into a structured row with:
- Execution timestamp, scenario name, environment
- HTTP method, URL, request/response payloads
- Pass/Fail status

### 3. S3 Ingestor (`util/karate_s3_ingestor.py`)

Converts flattened results to JSONL and uploads to S3.

```bash
# Auto-discover and process all reports
python util/karate_s3_ingestor.py

# Process a specific report file
python util/karate_s3_ingestor.py target/karate-reports/src.test.poc-log-generator.karate-json.txt
```

### 4. Athena MCP Server (`mcp/athena_mcp.py`)

Exposes Athena queries as an MCP tool so AI agents (Kiro) can run SQL against `karate_logs`.

Configure in `.kiro/settings/mcp.json`:

```json
{
  "mcpServers": {
    "athena-triage": {
      "command": "python",
      "args": ["mcp/athena_mcp.py"],
      "disabled": false,
      "autoApprove": ["run_athena_query"]
    }
  }
}
```

---

## Kiro Hook

A user-triggered hook is configured at `.kiro/hooks/karate-auto-ingest.kiro.hook` to run the full ingestion pipeline from within Kiro with one click.

---

## Quick Start

```bash
# 1. Install Python dependencies
pip install boto3 pandas mcp

# 2. Configure AWS credentials
aws configure

# 3. Create S3 bucket (if not exists)
aws s3 mb s3://gaurav-logs-archive

# 4. Create Athena database and table (see sections above)

# 5. Run Karate tests
mvn test -Dtest=net.gaurav.runner.PocLogGeneratorRunner -Dkarate.env=dev

# 6. Flatten and upload results to S3
python util/karate_s3_ingestor.py

# 7. Query results via Athena (or let Kiro do it via MCP)
```
