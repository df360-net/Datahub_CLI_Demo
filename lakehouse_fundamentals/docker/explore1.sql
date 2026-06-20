SELECT '=== 1. SNAPSHOTS (the current pointer + history) ===' AS section;
SELECT snapshot_id, operation, manifest_list FROM demo.smoke.t.snapshots;
SELECT '=== 2. MANIFEST FILES (which manifests this table has) ===' AS section;
SELECT path, added_data_files_count, existing_data_files_count FROM demo.smoke.t.manifests;
SELECT '=== 3. DATA FILES (the Parquet at the bottom) ===' AS section;
SELECT file_path, record_count, file_size_in_bytes FROM demo.smoke.t.files;
