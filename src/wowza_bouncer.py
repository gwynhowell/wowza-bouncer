""" wowza config data contains an auth token which must be refreshed
    at least once a hour. currently, we are unable to refresh the config
    without rebooting the server.
    
    this script bounces the wowza server every 5 minutes,
    unless it has been accessed within 30 minutes. 
"""


import csv
import datetime
import subprocess
import time


# bounce server every 5 minutes
SLEEP_SECONDS = 5 * 60
 
# prevent bounce if server was accessed within 30 minutes
BACKOFF_SECONDS = 30 * 60
 
# wowza log filepath
WOWZA_LOG_FILEPATH = 'wowzastreamingengine_access.log'
#WOWZA_LOG_FILEPATH = '/usr/local/WowzaStreamingEngine/logs/wowzastreamingengine_access.log'
WOWZA_START_FILEPATH = '/usr/local/WowzaStreamingEngine/bin/startup.sh'
WOWZA_STOP_FILEPATH = '/usr/local/WowzaStreamingEngine/bin/shutdown.sh'
 
# the event and category values to check in the log
CHECK_EVENT = 'connect'
CHECK_CATEGORY = 'session'
                
class WowzaBouncer(object):
    def __init__(self, verbose=False, **kwargs):
        self.sleep_seconds = kwargs.get('sleep_seconds', SLEEP_SECONDS)
        self.backoff_seconds = kwargs.get('backoff_seconds', BACKOFF_SECONDS)
         
        self.log_filepath = kwargs.get('log_filepath', WOWZA_LOG_FILEPATH)
        self.startup_filepath = kwargs.get('startup_filepath', WOWZA_START_FILEPATH)
        self.shutdown_filepath = kwargs.get('shutdown_filepath', WOWZA_STOP_FILEPATH)
         
        self.check_event = kwargs.get('check_event', CHECK_EVENT)
        self.check_category = kwargs.get('check_category', CHECK_CATEGORY)
        
        self.last_accessed = None
        self.verbose = verbose
    
    def log(self, msg):
        if self.verbose:
            print msg
    
    def run(self):
        while True:
            self.last_accessed = self.get_last_accessed()
            
            if not self.last_accessed:
                self.log('no access logs found - bouncing server')
                self.bounce()
            else:
                now = datetime.datetime.now()
                secs = (now - self.last_accessed).seconds
                
                if secs > self.backoff_seconds:
                    self.log('wowza not accessed for {0} seconds - bouncing server'.format(secs))
                    self.bounce()
                else:
                    self.log('wowza accessed {0} seconds ago - skipping bounce'.format(secs))
            
            time.sleep(self.sleep_seconds)
    
    def get_last_accessed(self):
        """ parse wowza logs to determine when it was last accessed """
        
        with open(self.log_filepath, 'r') as f:
            rows = list(csv.reader(f, delimiter='\t'))
            for row in reversed(rows):
                if len(row) < 4:
                    continue

                d = row[0] # eg 2015-07-13
                t = row[1] # eg 09:20:44
                event = row[3]
                category = row[4]
                
                if event == self.check_event and category == self.check_category:
                    dt = '{0}T{1}'.format(d, t) # eg 2015-07-13T09:20:44
                    dt = datetime.datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S')
                    return dt
        
        if self.verbose:
            print 'no access logs found'
    
    def bounce(self):
        """ restarts the server """
        
        self.log('- calling {0}'.format(self.shutdown_filepath))
        subprocess.call([self.shutdown_filepath])
        
        self.log('- calling {0}'.format(self.startup_filepath))
        subprocess.call([self.startup_filepath])
        
def main():
    bouncer = WowzaBouncer(verbose=True)
    bouncer.run()

if __name__ == '__main__':
    main()