"""Subway headway — 직접 빌드 배치 파이프라인 (build-then-validate).

WAP(staging→publish) 대신, 마트를 gold 에 **바로 빌드한 뒤 검증**하는 단순 경로.
WAP 파이프라인(subway_headway_wap_pipeline)과 비교용으로 따로 둔다.
  - 직접 빌드: 빠르고 단순. 검증은 빌드 *후*.
  - WAP:      staging 검증 통과 후에만 publish (나쁜 데이터 차단).

전제: 스트리밍 적재(bronze fanout) RUNNING, StarRocks iceberg_catalog 생성됨.
"""
from __future__ import annotations

import json
import urllib.request

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

FLINK = "subway-flink-jobmanager"
SPARK = "subway-spark-client"
FLINK_OVERVIEW_URL = "http://flink-jobmanager:8081/jobs/overview"

PKGS = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
    "org.apache.paimon:paimon-spark-3.5_2.12:1.4.1",
    "org.apache.paimon:paimon-s3:1.4.1",
])
NETTY = "-Dorg.apache.iceberg.shaded.io.netty.noUnsafe=true -Dio.netty.noUnsafe=true"
ENSURE = (
    "docker start subway-minio subway-iceberg-postgres subway-iceberg-rest "
    f"{FLINK} subway-flink-taskmanager {SPARK} >/dev/null 2>&1 || true"
)


def spark_mart(script: str) -> str:
    return (
        f"{ENSURE} && docker exec {SPARK} /opt/spark/bin/spark-submit "
        f"--packages '{PKGS}' "
        f"--conf spark.driver.memory=2g "
        f"--conf spark.sql.iceberg.vectorization.enabled=false "
        f"--conf \"spark.driver.extraJavaOptions={NETTY}\" "
        f"/workspace/{script}"
    )


def validate_bronze_running() -> None:
    with urllib.request.urlopen(FLINK_OVERVIEW_URL, timeout=10) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    running = [j for j in payload.get("jobs", []) if j.get("state") == "RUNNING"]
    names = [str(j.get("name", "")) for j in running]
    print("running_flink_jobs=" + json.dumps(names, ensure_ascii=False))
    if not any("subway-bronze-fanout" in n for n in names):
        raise AirflowException(f"bronze fanout 잡이 RUNNING 이 아님. 현재: {names}")
    print("OK: subway-bronze-fanout RUNNING")


with DAG(
    dag_id="subway_headway_pipeline",
    description="headway 배치(직접 빌드): 적재검증 → L1 도착 → gold 직접 빌드 → 서빙 검증",
    start_date=pendulum.datetime(2026, 6, 1, tz="Asia/Seoul"),
    schedule=None,                 # 비교용 — 수동 트리거
    catchup=False,
    max_active_runs=1,
    tags=["seoul-metro", "headway", "batch", "direct"],
    doc_md="""
## Subway headway — 직접 빌드 파이프라인

마트를 gold 에 바로 빌드하고 *후*에 검증한다(build-then-validate). WAP 와 비교용.

```text
validate_runtime → validate_bronze → l1_arrival
  → build_headway_mart → build_freshness_mart → validate_gold(서빙 행수)
```
""",
) as dag:

    start = EmptyOperator(task_id="start")

    validate_runtime = BashOperator(
        task_id="validate_runtime_services",
        bash_command="""
set -euo pipefail
docker exec subway-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list >/dev/null && echo kafka-ok
curl -fsS http://flink-jobmanager:8081/overview >/dev/null && echo flink-ok
curl -fsS http://minio:9000/minio/health/live && echo minio-ok
curl -fsS http://iceberg-rest:8181/v1/config >/dev/null && echo iceberg-ok
docker exec subway-starrocks-fe mysql -h127.0.0.1 -P9030 -uroot -e "SELECT 1;" >/dev/null && echo starrocks-ok
""",
    )

    validate_bronze = PythonOperator(
        task_id="validate_bronze_running",
        python_callable=validate_bronze_running,
    )

    l1_arrival_events = BashOperator(
        task_id="l1_arrival_events",
        bash_command=(
            f"{ENSURE} && docker exec {FLINK} /opt/flink/bin/sql-client.sh "
            f"-f /workspace/labs/12-arrival-events/01b-arrival-events-fallback.sql"
        ),
    )

    build_headway_mart = BashOperator(
        task_id="build_headway_mart",
        bash_command=spark_mart("labs/13-spark-headway/headway_mart.py"),
    )

    build_freshness_mart = BashOperator(
        task_id="build_freshness_mart",
        bash_command=spark_mart("labs/14-spark-freshness/freshness_mart.py"),
    )

    # 빌드 후 검증: 서빙 메타 새로고침 + gold 행수 ≥ 1
    validate_gold = BashOperator(
        task_id="validate_gold",
        bash_command="""
set -euo pipefail
docker exec subway-starrocks-fe mysql -h127.0.0.1 -P9030 -uroot -e "
REFRESH EXTERNAL TABLE iceberg_catalog.gold.subway_headway_by_station_tod;
REFRESH EXTERNAL TABLE iceberg_catalog.gold.subway_service_freshness;" || true
HW=$(docker exec subway-starrocks-fe mysql -N -h127.0.0.1 -P9030 -uroot -e "SELECT COUNT(*) FROM iceberg_catalog.gold.subway_headway_by_station_tod;")
FR=$(docker exec subway-starrocks-fe mysql -N -h127.0.0.1 -P9030 -uroot -e "SELECT COUNT(*) FROM iceberg_catalog.gold.subway_service_freshness;")
echo "headway_rows=$HW  freshness_rows=$FR"
[ "$HW" -ge 1 ] || { echo "gold headway 비어있음"; exit 1; }
[ "$FR" -ge 1 ] || { echo "gold freshness 비어있음"; exit 1; }
echo "OK: gold 서빙 데이터 존재"
""",
    )

    finish = EmptyOperator(task_id="finish")

    (
        start
        >> validate_runtime
        >> validate_bronze
        >> l1_arrival_events
        >> build_headway_mart
        >> build_freshness_mart
        >> validate_gold
        >> finish
    )
