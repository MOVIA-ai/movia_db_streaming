from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
import apache_beam as beam
from apache_beam import pvalue
from google.cloud import firestore
from datetime import datetime
import pytz
import logging
import time
import string, random
import pandas as pd

_LOGGER = logging.getLogger("__main__")

def rand_string(size = 16):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

    
class WriteToFirestore(beam.DoFn):
    db = firestore.Client(project="vivid-vent-265202")
    
    def process(self, element):
        device_hash, position = element
        doc_ref = self.db.collection("vehicles").document(device_hash)

        event_timestamp = datetime.fromtimestamp(position.get("event_timestamp"))
        event_timestamp_utc = event_timestamp.astimezone(pytz.utc)

        soc = None
        rawSoc = position.get("4")
        if rawSoc:
            soc = rawSoc/10
    
        position_map = {
            "speed": position.get("1"),
            "SoH": position.get("15"),
            "latitude": position.get("latitude"),
            "longitude": position.get("longitude"),
            "SoC": soc,
            "Odometer": position.get("21"),
            "event_timestamp": event_timestamp
        }

        position_map = {k: v for k, v in position_map.items() if v is not None}

        try:
            doc_ref.set(position_map, merge=True)
        except Exception as e:
            _LOGGER.error(f"Failed to update position for vehicle {device_hash}")
            _LOGGER.error(e)

class WindowLoggingFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        window_start = window.start.to_utc_datetime()
        window_end = window.end.to_utc_datetime()
        logging.info(f"Element processed in window: {window_start} to {window_end}")
        yield element


class BaseAlertClass(beam.DoFn):
    def alert_columns_selector(self, alert_kind):
        if alert_kind == 'speed':
            timestamp_col = 'speed_alert_ts'
            state_col     = 'speed_alert_state'
        elif alert_kind == 'soc':
            timestamp_col = 'bat_alert_ts'
            state_col     = 'bat_alert_state'
        else:
            #_LOGGER.warning(f'Unknown alert kind {alert_kind}')
            timestamp_col = alert_kind + '_alert_ts'
            state_col = alert_kind + '_alert_state'
            #timestamp_col = state_col = None

        return (timestamp_col, state_col)


    def update_alarm_state(self, device_hash, alert_kind, alert_state, alert_timestamp):
        '''
        device_hash     speed_alert_ts bat_alert_ts speed_alert_state bat_alert_state
        354762116168540            NaT          NaT         TRIGGERED            IDLE
        354762116136166            NaT          NaT              IDLE       TRIGGERED   
        354762116136166            NaT          NaT              IDLE            IDLE
        '''
        timestamp_col, state_col = self.alert_columns_selector(alert_kind)
        if not (timestamp_col or state_col):
            return

        doc_ref = self.db.collection("vehicles").document(device_hash)

        position_map = {timestamp_col: alert_timestamp, state_col: alert_state}
        doc_ref.set(position_map, merge=True)
        _LOGGER.warning(f'\n>>>>>>>>>> \n >>>>>>>>>> WRITING ALARM STATE {alert_kind} TO FIRESTORE \n >>>>>>>>>>')


    def get_alarm_state(self, device_hash, alert_kind, event_timestamp):
        
        timestamp_col, state_col = self.alert_columns_selector(alert_kind)
        if not (timestamp_col or state_col):
            return (None, None)

        doc_ref = self.db.collection("vehicles").document(device_hash)
        doc_snapshot = doc_ref.get()

        if not doc_snapshot.exists:
            _LOGGER.warning(f'Device {device_hash} doesn\'t have record in firestore setup yet. Setting {alert_kind} alert to IDLE.')
            self.update_alarm_state(device_hash, alert_kind, 'IDLE', event_timestamp)
            return ('IDLE', event_timestamp)

        try:
            alert_state = doc_snapshot.get(state_col)
        except KeyError:
            _LOGGER.warning(f'Device {device_hash} doesn\'t have an alert state for {alert_kind} setup yet. Setting to IDLE.')
            self.update_alarm_state(device_hash, alert_kind, 'IDLE', event_timestamp)
            return ('IDLE', event_timestamp)
        else:
            print(f'>>>>>>>>>> \n >>>>>>>>>> READING ALARM STATE {alert_kind} FROM FIRESTORE 2 \n >>>>>>>>>>')
            #if we fetched alert_state correctly, we can assume there's a timestamp for it.
            alert_ts = doc_snapshot.get(timestamp_col)
            return (alert_state, alert_ts)


    def fabricate_alert(self, vehicleId, typeId, severity, valorGenerado, valorUmbral, timestamp):
        alertId = rand_string() + "_pubsub"
        alert_dict = {
            'alertId': alertId,
            'vehicleId': vehicleId,
            'typeId': typeId,
            'severity': severity,
            'valorGenerado': valorGenerado,
            'valorUmbral': valorUmbral,
            'timestamp': timestamp
        }
        return alert_dict


    def setup(self):
        self.db = firestore.Client(project="vivid-vent-265202")


class CalculateOpsAlerts(BaseAlertClass):

    STATE_SPEC = ReadModifyWriteStateSpec('custom_spec', beam.coders.PickleCoder())

    def process(self, element, state = beam.DoFn.StateParam(STATE_SPEC)):

        _LOGGER.info(element)
        device_hash, position = element
        max_speed, min_soc, updated_soh, speed_threshold_for_device, soc_threshold_for_device, event_unix_timestamp = position 
        event_timestamp = datetime.fromtimestamp(event_unix_timestamp).astimezone(pytz.utc)

        #CHECK SoH ALERTS
        current_soh = state.read()

        if current_soh and updated_soh and updated_soh > 0 and updated_soh < current_soh:
            _LOGGER.info(f'SoH has decreased for vehicle {device_hash}!')
            yield self.fabricate_alert(device_hash, 'soh', '', updated_soh, current_soh, event_timestamp)

        if updated_soh:
            state.write(current_soh)

        MIN_SOC_TIME_WINDOW = 60*20 #20 min
        MAX_SPEED_TIME_WINDOW = 60*5  #5 min 

        #CHECK SoC ALERTS
        soc = min_soc/10 if min_soc else None
        if soc and soc > 0:
            soc_alarm_state, soc_alarm_ts = self.get_alarm_state(device_hash, 'soc', event_unix_timestamp)

            if soc_alarm_state and soc_alarm_state != 'TRIGGERED':
                if soc < soc_threshold_for_device:
                    self.update_alarm_state(device_hash, 'soc', 'TRIGGERED', event_unix_timestamp)
                    yield self.fabricate_alert(device_hash, 'soc', '', soc, soc_threshold_for_device, event_timestamp)
            else: #check if it is time to reset the alert 
                current_timestamp = time.time()
                if not soc_alarm_ts:
                    soc_alarm_ts = 0
                if current_timestamp - soc_alarm_ts > MIN_SOC_TIME_WINDOW and soc >= soc_threshold_for_device:
                    self.update_alarm_state(device_hash, 'soc', 'IDLE', event_unix_timestamp)


        #CHECK Speed ALERTS
        if max_speed:
            speed_alarm_state, speed_alarm_ts = self.get_alarm_state(device_hash, 'speed', event_unix_timestamp)

            if speed_alarm_state and speed_alarm_state != 'TRIGGERED':
                if max_speed > speed_threshold_for_device:
                    self.update_alarm_state(device_hash, 'speed', 'TRIGGERED', event_unix_timestamp)
                    yield self.fabricate_alert(device_hash, 'speed', '', max_speed, speed_threshold_for_device, event_timestamp)
            else: 
                current_timestamp = time.time()
                if not speed_alarm_ts:
                    speed_alarm_ts = 0
                if current_timestamp - speed_alarm_ts > MAX_SPEED_TIME_WINDOW and max_speed <= speed_threshold_for_device:
                    self.update_alarm_state(device_hash, 'speed', 'IDLE', event_unix_timestamp)



class CalculateGeoAlerts(BaseAlertClass):

    STATE_SPEC = ReadModifyWriteStateSpec('custom_spec', beam.coders.PickleCoder())
    MINIMUM_TIME_THRESHOLD = 300
    def convert_to_coords(self, polygon_string):
        """
        Converts a polygon string of the form "lat1;lon1,lat2;lon2,..." to a list of (lat, lon) tuples.
        """
        points = polygon_string.split(',')
        coords = [(float(p.split(';')[0]), float(p.split(';')[1])) for p in points]
        
        return coords

    def enough_time_has_passed(self, data_point, time_thresh):
        #get message timestamp
        msg_ts = data_point['event_timestamp']

        #get first time outside tiemstamp
        first_out_tstamp = self.state['out_ts']

        has_initialized_inside = self.state.get('in_ts') is not None #or False not pd.isna(self.state['in_ts'])

        return ((msg_ts - first_out_tstamp) > time_thresh) and has_initialized_inside

    def state_rel_to_polygon(self, data_point):
        def is_inside(data_point, polygon):
            """
            Determines if a point is inside a polygon using the ray casting algorithm.
            """
            if ('latitude' in data_point) and ('longitude' in data_point):

                x = float(data_point['latitude'])
                y = float(data_point['longitude'])

                odd_nodes = False
                j = len(polygon) - 1  # The last vertex is the previous vertex to the first

                for i in range(len(polygon)):
                    xi, yi = polygon[i]
                    xj, yj = polygon[j]
                    if yi < y and yj >= y or yj < y and yi >= y:
                        if xi + (y - yi) / (yj - yi) * (xj - xi) < x:
                            odd_nodes = not odd_nodes
                    j = i

                return odd_nodes

            else: 
                return False    

        if is_inside(data_point, self.convert_to_coords(data_point['polygon'])):
            return 'INSIDE'
        
        else: return 'OUTSIDE'

    def get_current_state(self):
        # UNDEFINED, INSIDE, OUTSIDE
        current_geo_state = self.state or None
        if self.state is None:
            self.state = {}

        if 'geo_alert_state' in self.state:
            current_geo_state = self.state['geo_alert_state']
        else:
            current_geo_state = self.state['geo_alert_state'] = 'UNDEFINED'

        return current_geo_state

    def process(self, element, state = beam.DoFn.StateParam(STATE_SPEC)):
        #this is a state machine. It can go from INSIDE -> OUTSIDE; and from OUTSIDE -> INSIDE

        alert_id, position = element
        device_hash = position['device_hash']
        
        _LOGGER.info(f'getting element: {element}')
        self.state = state.read()
        _LOGGER.info(f'state is: {self.state}')
        
        #get current vehicle state
        cur_veh_state = self.get_current_state()
        
        #check if vehicle is INSIDE / OUTSIDE
        new_veh_state = self.state_rel_to_polygon(position)

        #update values and emit alert if necessary
        # INSIDE  - >  OUTSIDE
        if cur_veh_state == 'INSIDE' and new_veh_state == 'OUTSIDE':
            #update out_ts
            _LOGGER.debug('update out_ts')
            self.state['out_ts'] = position['event_timestamp']

        # INSIDE  - >  INSIDE
        elif cur_veh_state == 'INSIDE' and new_veh_state == 'INSIDE':
            #update last_time_inside (if needed) 
            _LOGGER.debug(f'update last_time_inside (if needed)')
            if position['event_timestamp'] > self.state['in_ts']:
                self.state['in_ts'] = position['event_timestamp']

        # OUTSIDE - >  OUTSIDE
        elif new_veh_state =='OUTSIDE':
            if cur_veh_state == 'OUTSIDE':
                _LOGGER.debug(f'update fist_time outside (if needed)')
                #update fist_time outside (if needed) ----------------(not self.state['out_ts']) or (
                if position['event_timestamp'] < self.state['out_ts']: \
                    self.state['out_ts'] = position['event_timestamp'] 

                #check if it is necessary to emit alert
                alarm_state = self.get_alarm_state(device_hash, 'geofence_' + str(alert_id), position['event_timestamp'])
                if alarm_state != 'TRIGGERED' and self.enough_time_has_passed(position, self.MINIMUM_TIME_THRESHOLD):
                    self.update_alarm_state(device_hash, 'geofence_' + str(alert_id), 'TRIGGERED', position['event_timestamp'])
                    yield self.fabricate_alert(device_hash, 'geofence', '', str(alert_id), None, position['event_timestamp'])

            # UNDEFINED - >  OUTSIDE
            else:
                #update fist_time outside
                _LOGGER.info(f'else update fist_time outside {self.state}')
                self.state['out_ts'] = position['event_timestamp']

        # OUTSIDE - >  INSIDE or UNDEFINED - >  INSIDE
        elif new_veh_state == 'INSIDE':
            #reset alert state
            self.update_alarm_state(device_hash, 'geofence_' + str(alert_id), 'IDLE', position['event_timestamp'])
            self.state['in_ts'] = position['event_timestamp']

        else:
            _LOGGER.error(f'uncaught state transition {cur_veh_state} -->> {new_veh_state}')

        #update new vehicle state
        self.state['geo_alert_state'] = new_veh_state
        _LOGGER.info(f'state is: {self.state}')
        state.write(self.state)




    
