# DEPRECATED — 이 단일 DAG 는 두 개로 분리되었습니다.
#   - subway_headway_wap_pipeline.py  (WAP: staging→audit→publish→validate)
#   - subway_headway_pipeline.py      (직접 빌드: build-then-validate)
# DAG 객체가 없으므로 Airflow 는 이 파일에서 아무 DAG 도 로드하지 않습니다.
# 이 파일은 탐색기나 `rm` 으로 삭제해도 됩니다.
