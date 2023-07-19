-- Variables definidas en el UI de Airflow
put file://{{params.path_file}}@{{params.stage}} auto_compress=true;