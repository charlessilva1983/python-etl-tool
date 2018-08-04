# example queries, will be different across different db platform
mysql_extract_t0 = "SELECT count(1) FROM <table_x>"
mysql_extract_t1 = "CALL Perform_TaskX(@p0)"
mysql_extract_t2 = "CALL Perform_TaskY(@p0)"
mysql_validation = "SELECT @p0 AS rows"

mysql_queries = {'t0': mysql_extract_t0, 't1': mysql_extract_t1, 't2': mysql_extract_t2 }
mysql_queries['valid'] = mysql_validation