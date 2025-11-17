def data_router(**kwargs):
    ti = kwargs['ti']
    xcom_value = ti.xcom_pull(task_ids='validate_data_source', key='return_value')

    try:
        record_count = int(xcom_value) if xcom_value is not None else 0
    except (TypeError, ValueError):
        record_count = 0

    if record_count >= 100:
        return 'process_data_in_production'
    else:
        return 'send_low_record_warning'