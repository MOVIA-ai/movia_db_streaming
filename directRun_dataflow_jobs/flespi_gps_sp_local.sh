python pipeline_flespi.py \
--streaming \
--input_subscription projects/vivid-vent-265202/subscriptions/gps_and_sensor_flespi \
--output_table_sp movia-439221:prd_movia.sp_point \
--output_table_gps movia-439221:prd_movia.gps_point \
--output_table_errors movia-439221:prd_movia.bq_insertion_errors \
--sp_schema "asset_device_fk:INT64,event_timestamp:TIMESTAMP,created:TIMESTAMP,updated:TIMESTAMP,sensor_type_id:INTEGER,value:FLOAT"   \
--gps_schema "asset_device_fk:INT64,event_timestamp:TIMESTAMP,created:TIMESTAMP,updated:TIMESTAMP,speed:FLOAT,latitude:FLOAT,longitude:FLOAT,altitude:FLOAT,extras:STRING" \
--runner DirectRunner --local_logging