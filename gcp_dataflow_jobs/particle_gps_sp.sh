python pipeline_particle.py --update \
--streaming \
--input_subscription_gps projects/vivid-vent-265202/subscriptions/gpspoint-particle-sub_prd \
--input_subscription_sp projects/vivid-vent-265202/subscriptions/sensorpoint-particle-sub_prd \
--output_table_gps vivid-vent-265202:prd_movia.gps_point_particle \
--output_table_sp vivid-vent-265202:prd_movia.sensor_point_particle \
--output_schema_gps "device_id:STRING,published_at:TIMESTAMP,event:STRING,t:TIMESTAMP,h:INTEGER,s:FLOAT,a:INTEGER,lg:FLOAT,lt:FLOAT,V:INTEGER,extras:STRING" \
--output_schema_sp "device_id:STRING,published_at:TIMESTAMP,event:STRING,t:TIMESTAMP,s:INTEGER,v:FLOAT,extras:STRING" \
--output_table_errors vivid-vent-265202:prd_movia.bq_insertion_errors \
--runner DataflowRunner \
--project vivid-vent-265202 \
--temp_location gs://temporary_pubsub_particle/temp \
--job_name particle-pipeline \
--max_num_workers 4 \
--region us-central1 \
--worker_machine_type e2-small


