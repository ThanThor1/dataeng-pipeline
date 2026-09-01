#!/bin/sh
# docker/minio/init-buckets.sh
# Chạy bởi container minio-init, dùng mc (MinIO Client) để tạo bucket

set -e

echo ">> Cấu hình mc alias trỏ tới MinIO server..."
mc alias set localminio "${MINIO_ENDPOINT_INTERNAL}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"

echo ">> Tạo bucket cho Medallion Architecture..."
for bucket in "${MINIO_BUCKET_BRONZE}" "${MINIO_BUCKET_SILVER}" "${MINIO_BUCKET_GOLD}"; do
  if mc ls "localminio/${bucket}" >/dev/null 2>&1; then
    echo "   - Bucket '${bucket}' đã tồn tại, bỏ qua."
  else
    mc mb "localminio/${bucket}"
    echo "   - Đã tạo bucket '${bucket}'."
  fi
done

echo ">> Danh sách bucket hiện có:"
mc ls localminio

echo ">> Hoàn tất init MinIO buckets."
