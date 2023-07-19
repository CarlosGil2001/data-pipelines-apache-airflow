-- Variables definidas en el UI de Airflow
--Copiar el archivo CSV a la tabla
copy into {{ params.table }} from @{{ params.stage }}/premier_position.csv.gz FILE_FORMAT=(TYPE=csv field_delimiter=',' skip_header=1) ON_ERROR = 'CONTINUE';