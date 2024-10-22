python pipeline_flespi.py \
--streaming \
--input_subscription projects/vivid-vent-265202/subscriptions/sensor_gpspoint-flespi-sub_prd \
--output_table_sp vivid-vent-265202:prd_movia.sensor_point_flespi \
--output_table_gps vivid-vent-265202:prd_movia.gps_point_flespi \
--output_table_errors vivid-vent-265202:prd_movia.bq_insertion_errors \
--sp_schema "device_hash:STRING,event_timestamp:TIMESTAMP,created:TIMESTAMP,updated:TIMESTAMP,sensor_type_id:INTEGER,value:FLOAT"   \
--gps_schema "device_hash:STRING,event_timestamp:TIMESTAMP,created:TIMESTAMP,updated:TIMESTAMP,speed:FLOAT,latitude:FLOAT,longitude:FLOAT,altitude:FLOAT,extras:STRING" \
--runner DirectRunner --local_logging