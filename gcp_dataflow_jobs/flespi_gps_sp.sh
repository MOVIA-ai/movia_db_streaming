python pipeline_flespi.py \
--update \
--input_subscription projects/movia-439221/subscriptions/sensor_gpspoint-flespi-sub_prd \
--output_table_sp movia-439221:prd_movia.sp_point \
--output_table_gps movia-439221:prd_movia.gps_point \
--output_table_errors movia-439221:prd_movia.bq_insertion_errors \
--sp_schema "asset_device_fk:INT64,event_timestamp:TIMESTAMP,created:TIMESTAMP,updated:TIMESTAMP,sensor_type_id:INTEGER,value:FLOAT"   \
--gps_schema "asset_device_fk:INT64,event_timestamp:TIMESTAMP,created:TIMESTAMP,updated:TIMESTAMP,speed:FLOAT,latitude:FLOAT,longitude:FLOAT,altitude:FLOAT,extras:STRING" \
--runner DataflowRunner \
--project movia-439221 \
--temp_location gs://movia_temporary_pubsub_flespi/temp \
--job_name sensorgpspoint-flespi-pipeline \
--max_num_workers 5 \
--region us-central1 \
--worker_machine_type e2-small \
--streaming \
--requirements_file requirements.txt \
--save_main_session \
--setup_file ./setup.py \
# --prebuild_sdk_container_engine=cloud_build \
# --sdk_container_image=gcr.io/vivid-vent-265202/beam-python-sdk:latest \
# --sdk_location=container


#--transform_name_mapping '{"WriteToBigQuery1":"WriteToBigQuery_SP", "WriteToBigQuery2":"WriteToBigQuery_GPS"}'
