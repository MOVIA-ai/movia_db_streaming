python pipeline_flespi.py --update \
--streaming \
--input_subscription projects/vivid-vent-265202/subscriptions/sensor_gpspoint-flespi-sub_prd \
--output_table_sp vivid-vent-265202:prd_movia.sensor_point_flespi \
--output_table_gps vivid-vent-265202:prd_movia.gps_point_flespi \
--output_table_errors vivid-vent-265202:prd_movia.bq_insertion_errors \
--sp_schema "asset_device_fk:INT64,event_timestamp:TIMESTAMP,created:TIMESTAMP,updated:TIMESTAMP,sensor_type_id:INTEGER,value:FLOAT" \
--gps_schema "asset_device_fk:INT64,event_timestamp:TIMESTAMP,created:TIMESTAMP,updated:TIMESTAMP,speed:FLOAT,latitude:FLOAT,longitude:FLOAT,altitude:FLOAT,extras:STRING,hdop:FLOAT,direction:FLOAT,vehicle_mileage:FLOAT,satellites:INTEGER" \
--runner DataflowRunner \
--project vivid-vent-265202 \
--temp_location gs://temporary_pubsub_flespi/temp \
--job_name sensorgpspoint-flespi-firebase-pipeline \
--max_num_workers 5 \
--region us-central1 \
--worker_machine_type e2-small \
--sdk_container_image=gcr.io/vivid-vent-265202/beam-python-sdk:latest \
--sdk_location=container


#--transform_name_mapping '{"WriteToBigQuery1":"WriteToBigQuery_SP", "WriteToBigQuery2":"WriteToBigQuery_GPS"}'
#--requirements_file requirements.txt \