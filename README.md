# Scala 2 Spark Devcontainer

This repository is a ready-to-open Scala 2 development workspace for building
and running an Apache Spark CSV pipeline. It includes:

- Scala `2.13.18`
- Apache Spark SQL `4.1.1`
- sbt `1.12.11`
- a VS Code Dev Containers setup with JDK 21, Scala CLI, sbt, Metals, Codex,
  and Metals MCP
- JDK source archives linked into the devcontainer JDK so Metals can navigate
  into Java standard library classes
- a production Dockerfile that builds a runnable assembly JAR

The example application starts a local Spark standalone cluster with one master
and three worker nodes, reads a transaction CSV with a fixed schema, validates
and enriches rows, aggregates revenue metrics, and writes partitioned Parquet
output.

## Quick Start

Open the folder in VS Code and run **Dev Containers: Reopen in Container**.

Inside the container:

```bash
scala-cli scripts/GenerateTransactions.scala -- data/input/transactions.csv 100000
sbt -Dsbt.batch=true run
sbt -Dsbt.batch=true "run data/input/transactions.csv target/spark-output/transaction-summary"
```

Build and run the application image:

```bash
docker build -t spark-devcontainer .
docker run --rm spark-devcontainer
```

The default input is:

```text
data/input/transactions.csv
```

The default output is:

```text
target/spark-output/transaction-summary
```

The default Spark master is:

```text
local-cluster[3,1,1024]
```

That Spark master starts one local standalone master and three worker JVMs, each
with one core and 1024 MiB of worker memory. Use this mode when you want the
template to behave like a small cluster without managing external services.

## CSV Contract

The pipeline uses an explicit Spark `StructType`; it does not infer CSV types.
Input files must include this header:

```csv
transaction_id,customer_id,event_ts,region,country,product_id,product_category,quantity,unit_price,discount_pct,payment_method,status
```

Field meanings:

- `transaction_id`: unique transaction identifier
- `customer_id`: customer identifier
- `event_ts`: timestamp in `yyyy-MM-dd'T'HH:mm:ss` format
- `region`: business region, such as `NA`, `EMEA`, or `APAC`
- `country`: ISO-like country code used for reporting
- `product_id`: product SKU
- `product_category`: reporting category
- `quantity`: positive integer quantity
- `unit_price`: non-negative item price
- `discount_pct`: decimal discount from `0.0` through `1.0`
- `payment_method`: payment channel label
- `status`: transaction status, such as `completed` or `refunded`

Malformed rows, rows with missing identifiers, invalid timestamps, non-positive
quantities, negative prices, or discounts outside `0.0` through `1.0` are
excluded before aggregation.

Regenerate the checked-in 100,000-row sample file with:

```bash
scala-cli scripts/GenerateTransactions.scala -- data/input/transactions.csv 100000
```

## Pipeline Output

The job writes Parquet files partitioned by `event_date`. Each output row is
grouped by:

- `event_date`
- `region`
- `country`
- `product_category`
- `status`

Metrics written:

- `transaction_count`
- `unique_customers`
- `units_sold`
- `gross_revenue`
- `net_revenue`

For a large CSV, pass the input and output paths explicitly:

```bash
sbt -Dsbt.batch=true "run /data/landing/transactions.csv /data/curated/transaction-summary"
```

To target a different Spark master, pass a third argument:

```bash
sbt -Dsbt.batch=true "run /data/landing/transactions.csv /data/curated/transaction-summary spark://spark-master:7077"
```

## Build And Validation

Run sbt commands serially. Starting multiple sbt processes at the same time can
hit the sbt boot socket lock and fail with `ServerAlreadyBootingException`.

```bash
sbt -Dsbt.batch=true compile
sbt -Dsbt.batch=true run
sbt -Dsbt.batch=true assembly
sbt -Dsbt.batch=true scalafmtCheckAll
```

Format Scala sources with:

```bash
sbt -Dsbt.batch=true scalafmtAll
```

## Important Files

- `build.sbt`: pins Scala, adds Spark SQL, configures Java module options for
  Spark, and builds an assembly JAR.
- `src/main/scala/Main.scala`: Spark CSV pipeline entry point.
- `scripts/GenerateTransactions.scala`: deterministic generator for the
  checked-in transaction CSV sample.
- `data/input/transactions.csv`: 100,000-row sample file with the production
  CSV structure.
- `.devcontainer/Dockerfile`: development image with JDK 21, JDK sources for
  Metals Java navigation, Scala tools, and a minimal `/opt/spark` home used by
  local cluster executors.
- `Dockerfile`: production multi-stage build for the Spark application. It
  copies the same minimal Spark home into the runtime image so
  `local-cluster[3,1,1024]` can launch worker executors.
- `.vscode/launch.json`: Metals/Scala debug launch config for `Main`.

## Notes

Spark runs in `local-cluster[3,1,1024]` mode by default, which starts one local
standalone master and three workers for development. For an external cluster,
pass the Spark master URL as the third application argument and ensure the
input/output paths are accessible to the driver and executors.
