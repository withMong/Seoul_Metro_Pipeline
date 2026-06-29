"""Subway headway — WAP(Write-Audit-Publish) 배치 파이프라인.

스트리밍 적재(bronze fanout)는 이 DAG가 건드리지 않는다. 적재는 상시 RUNNING 이어야 하고,
이 DAG는 **적재 검증 → 도착 추출(L1) → staging 빌드 → 검증(GE) → publish → 재검증**만 한다.

  검증 통과(audit) 전에는 serving(gold)에 절대 반영하지 않는다.
  직접 빌드 방식과 비교하려면 → subway_headway_pipeline (build-then-validate).
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

# 필요한 컨테이너 깨우기(이미 떠 있으면 no-op)
ENSURE = (
    "docker start subway-minio subway-iceberg-postgres subway-iceberg-rest "
    f"{FLINK} subway-flink-taskmanager {SPARK} >/dev/null 2>&1 || true"
)


def wap_stage(stage: str) -> str:
    return (
        f"{ENSURE} && docker exec {SPARK} /opt/spark/bin/spark-submit "
        f"--packages '{PKGS}' "
        f"--conf spark.driver.memory=2g "
        f"--conf spark.sql.iceberg.vectorization.enabled=false "
        f"--conf \"spark.driver.extraJavaOptions={NETTY}\" "
        f"/workspace/labs/13-spark-headway/headway_wap.py --stage {stage}"
    )


def validate_bronze_running() -> None:
    """적재(streaming)가 살아있는지 — bronze fanout 잡이 RUNNING 이어야 배치 의미 있음."""
    with urllib.request.urlopen(FLINK_OVERVIEW_URL, timeout=10) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    running = [j for j in payload.get("jobs", []) if j.get("state") == "RUNNING"]
    names = [str(j.get("name", "")) for j in running]
    print("running_flink_jobs=" + json.dumps(names, ensure_ascii=False))
    if not any("subway-bronze-fanout" in n for n in names):
        raise AirflowException(f"bronze fanout 잡이 RUNNING 이 아님. 현재: {names}")
    print("OK: subway-bronze-fanout RUNNING")


with DAG(
    dag_id="subway_headway_wap_pipeline",
    description="headway 배치 WAP: 적재검증 → L1 도착 → staging → audit(GE) → publish → validate",
    start_date=pendulum.datetime(2026, 6, 1, tz="Asia/Seoul"),
    schedule="0 2 * * *",          # 매일 02:00 (수집 끝난 뒤)
    catchup=False,
    max_active_runs=1,
    tags=["seoul-metro", "headway", "batch", "wap"],
    doc_md="""
## Subway headway — WAP 파이프라인

스트리밍 적재와 분리된 **배치 집계**. 검증(audit, Great Expectations) 통과 시에만
serving(gold)으로 publish 한다.

```text
validate_runtime → validate_bronze → l1_arrival
  → wap_write(staging) → wap_audit(GE) → wap_publish(→gold) → wap_validate
                              │ 실패 시 멈춤 → gold 그대로 유지
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

    wap_write = BashOperator(task_id="wap_write", bash_command=wap_stage("write"))
    wap_audit = BashOperator(task_id="wap_audit", bash_command=wap_stage("audit"))
    wap_publish = BashOperator(task_id="wap_publish", bash_command=wap_stage("publish"))
    wap_validate = BashOperator(task_id="wap_validate", bash_command=wap_stage("validate"))

    finish = EmptyOperator(task_id="finish")

    (
        start
        >> validate_runtime
        >> validate_bronze
        >> l1_arrival_events
        >> wap_write
        >> wap_audit
        >> wap_publish
        >> wap_validate
        >> finish
    )
