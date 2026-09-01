#!/bin/bash
# scripts/check_minio_buckets.sh
# Kiểm tra bucket MinIO đã được tạo đúng chưa

echo ">> Danh sách bucket trong MinIO:"
docker exec minio-init mc ls localminio 2>/dev/null || \
docker run --rm --network dataeng_network minio/mc:latest \
  sh -c "mc alias set localminio http://minio:9000 minioadmin minioadmin123 && mc ls localminio"
