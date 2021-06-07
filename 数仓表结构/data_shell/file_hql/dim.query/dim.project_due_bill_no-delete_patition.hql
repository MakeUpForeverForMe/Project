-- 债转错误时删除
ALTER TABLE dim.project_due_bill_no DROP IF EXISTS PARTITION (import_id = '${exe_id}');
