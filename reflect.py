"""
Event/stats server collation and reflection.

"""
import json
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.httpclient

# Singleton used to track stream data
streams = {}


class KeyHandler(object):
    """
    Handle various key/values supplied by EVENT via dispatch()
    """
    def dispatch(self, stream, key, value):
        if hasattr(self, 'key_%s' % key):
            getattr(self, 'key_%s' % key)(stream, value)

    def key_new(self, stream, value):
        pass

    def key_listeners(self, stream, value):
        if stream != 'global':
            streams[stream]['listeners'] = int(value)
    
    def key_connected(self, stream, value):
        streams[stream]['connected'] = int(value)

    def key_title(self, stream, value):
        streams[stream]['title'] = value

    def key_listener_peak(self, stream, value):
        streams[stream]['listener_peak'] = int(value)


class EventHandler(object):
    """
    Manage the connection to the event/stats server. Auto-reconnects when it loses connection.
    """
    def __init__(self, url):
        self.url = url
        self.buffer = ""
        self.client = None
        self.handler = KeyHandler()
        self.backoff = 1

    def connect(self):
        print "Connecting to stats server"
        self.client = tornado.httpclient.AsyncHTTPClient()
        self.client.fetch(self.url, method="STATS", allow_nonstandard_methods=True, body="", streaming_callback=self.handle_update, callback=self.handle_disconnect)

    def handle_disconnect(self, *args):
        # Got a forced disconnect from the stats server
        print "Disconnected from stats server. Attempting reconnect in %d seconds" % self.backoff
        tornado.ioloop.IOLoop.instance().add_timeout(self.backoff, self.connect)

        # Increase backoff
        self.backoff *= 2

    def handle_update(self, content):
        # Successful data receive. Update backoff factor to 1s
        self.backoff = 1

        # Because we can receive partial lines in the update
        # We add the current buffer to the content
        lines = (self.buffer + content).split("\n")
        # Then we take the last line (partial or empty) and
        # push it into the buffer for next request
        self.buffer = lines[-1]

        for line in lines[:-1]:
            # Remove any crap
            line = line.strip()
            print line

            # Split up into values
            values = line.split(' ',4)

            # Less than 2 values and we don't know what's going on, ignore
            if len(values) < 2:
                continue

            (action, stream) = values[:2]

            # Dispatch to actions
            action = action.lower()
            if hasattr(self, 'action_%s' % action):
                # Update for any stream we don't know about
                if not streams.has_key(stream):
                        streams[stream] = {}

                getattr(self, 'action_%s' % action)(*values)
            
    def action_new(self, action, stream, *args):
        """ Handle new stream """
        streams[stream]['connected'] = 1

    def action_delete(self, action, stream, *args):
        """ Handle deleting a stream """
        streams[stream]['connected'] = 0
        streams[stream]['listeners'] = 0
        streams[stream]['title'] = ""

    def action_event(self, action, stream, key, value="", *args):
        """ Dispatch event data to recorder """
        self.handler.dispatch(stream, key, value)


class MainHandler(tornado.web.RequestHandler):
    """
    Any GET request will just give a JSON dump of the full set
    """
    def get(self, *args, **kwargs):
        self.write(json.dumps(streams))

        
# Route / to Main
application = tornado.web.Application([
    (r"/", MainHandler),
])

# Set up server and start service request
if __name__ == "__main__":
    server = tornado.httpserver.HTTPServer(application)
    server.listen(8888)

    service = EventHandler('http://admin:XXXXX@xxx.xxxx.xxxx.com:8000')
    service.connect()
    tornado.ioloop.IOLoop.instance().start()