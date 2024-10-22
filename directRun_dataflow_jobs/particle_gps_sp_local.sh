python pipeline_particle.py \
--streaming \
--input_subscription_gps projects/vivid-vent-265202/subscriptions/gpspoint-particle-sub_prd \
--input_subscription_sp projects/vivid-vent-265202/subscriptions/sensorpoint-particle-sub_prd \
--output_table_gps vivid-vent-265202:prd_movia.gps_point_particle \
--output_table_sp vivid-vent-265202:prd_movia.sensor_point_particle \
--output_table_errors vivid-vent-265202:prd_movia.bq_insertion_errors \
--output_schema_gps "device_id:STRING,published_at:TIMESTAMP,event:STRING,t:TIMESTAMP,h:INTEGER,s:FLOAT,a:INTEGER,lg:FLOAT,lt:FLOAT,V:INTEGER,extras:STRING" \
--output_schema_sp "device_id:STRING,published_at:TIMESTAMP,event:STRING,t:TIMESTAMP,s:INTEGER,v:FLOAT,extras:STRING" \
--runner DirectRunner