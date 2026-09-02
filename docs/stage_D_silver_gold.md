# Giai đoạn D: Spark làm sạch -> Silver, tính aggregation -> Gold

## Silver (đã làm sạch)
- orders_cleaning.py: ép timestamp, loại dòng hỏng, dedup theo order_id, GIỮ items dạng array lồng
- payments_cleaning.py: như trên + CHỈ GIỮ payment_status=success
- inventory_cleaning.py: như trên + loại quantity_on_hand < 0
- customers: KHÔNG có Silver job - để dành cho dbt snapshot (SCD2) ở Giai đoạn G

## Gold (đã tổng hợp)
- revenue_5min_window.py: SUM(amount) theo tumbling window 5 phút, nguồn Silver payments (success only)
- top_products_hourly.py: EXPLODE items, SUM(quantity) theo product + tumbling window 1 giờ
- customer_churn_signal.py: COUNT/SUM đơn hàng theo customer_id + tumbling window 1 giờ
  (dữ liệu NỀN, việc suy luận churn thật sự để dành cho dbt mart / BI)

## Quyết định thiết kế quan trọng
- explode() chỉ làm ở Gold, không làm ở Silver (tránh nhân đôi total_amount)
- outputMode("update") bắt buộc cho mọi job có windowed aggregation (khác append của Bronze/Silver)
- watermark 10 phút dùng nhất quán cho dedup (Silver) và windowed agg (Gold)
- Gold không tự tính "top N" hay "churn cuối cùng" - chỉ tổng hợp sẵn sàng cho tầng đọc lại

## Bài học vận hành quan trọng
- Laptop cá nhân không đủ tài nguyên cho 7+ streaming job đồng thời -> chuyển sang EC2 (t3.xlarge)
- Core và Memory là 2 chiều tài nguyên độc lập, cần tính riêng
- shuffle (dropDuplicates, groupBy) tạo nhiều file nhỏ do spark.sql.shuffle.partitions mặc định 200
- Luôn verify bằng count() dòng thật, không chỉ nhìn số object trên MinIO Console
