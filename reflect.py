#!/usr/bin/env python
"""
Event/stats server collation and reflection.

"""
import json
from optparse import OptionParser
import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.httpclient
import sys

# Singleton used to track stream data
streams = {}


class EventHandler(object):
    """
    Handle various key/values supplied by EVENT via dispatch()
    """

    def dispatch(self, stream, key, value):
        """
        Dispatch to key handler

        @param stream: Stream QName
        @param key: Event key
        @param value: Event value
        """

        handler = getattr(self, 'key_%s' % key.lower(), None)
        if handler:
            handler(stream, value)

    def key_new(self, stream, value):
        """
        Handle new event - currently ignored

        @param stream: Stream QName
        @param value: Value of new
        """
        pass

    def key_listeners(self, stream, value):
        """
        Update listeners value for stream

        @param stream: Stream QName
        @param value: Number of listeners
        """

        if stream != 'global':
            streams[stream]['listeners'] = int(value)
    
    def key_connected(self, stream, value):
        """
        Update connected value for stream

        @param stream: Stream QName
        @param value: Number of connections
        """

        streams[stream]['connected'] = int(value)

    def key_title(self, stream, value):
        """
        Update title value for stream

        @param stream: Stream QName
        @param value: Stream title
        """

        streams[stream]['title'] = value

    def key_listener_peak(self, stream, value):
        """
        Update listener_peak value for stream

        @param stream: Stream QName
        @param value: Number of listeners at peak
        """

        streams[stream]['listener_peak'] = int(value)


class ActionHandler(object):
    """
    Dispatch actions (NEW, EVENT, DELETE)
    """

    def __init__(self):
        self.event_handler = EventHandler()

    def dispatch(self, action, stream, *args):
        """
        Dispatch to the relevant action handler

        @param action: Action to dispatch for
        @param stream: Current stream
        @param args: Any other arguments for the action (EVENT particularly)
        @return:
        """
        
        # Find handler
        handler = getattr(self, 'action_%s' % action.lower(), None)
        if handler:
            # Update for any stream we don't know about
            if not streams.has_key(stream):
                    streams[stream] = {}

            handler(stream, *args)

    def action_new(self, stream, *args):
        """
        Handle a new stream

        @param stream: QName of stream
        @param args: Ignored
        @return:
        """

        streams[stream]['connected'] = 1

    def action_delete(self, stream, *args):
        """
        Handle removing a stream

        @param stream: QName of stream
        @param args: Ignored
        @return:
        """

        streams[stream]['connected'] = 0
        streams[stream]['listeners'] = 0
        streams[stream]['title'] = ""

    def action_event(self, stream, key, value="", *args):
        """
        Handle an event (information) on a stream

        @param stream: QName of stream
        @param key: Key for event
        @param value: Value for event
        @param args: Ignored
        @return:
        """

        self.event_handler.dispatch(stream, key, value)


class ServiceHandler(object):
    """
    Manage the connection to the event/stats server. Auto-reconnects when it loses connection.
    """

    def __init__(self, url, verbose=False):
        self.url = url
        self.verbose = verbose
        self.buffer = ""
        self.client = None
        self.action_handler = ActionHandler()
        self.backoff = 1
        self.connected = False

    def connect(self):
        """
        Connect to stats server

        """
        print "Connecting to stats server"
        self.connected = False
        self.client = tornado.httpclient.AsyncHTTPClient()
        self.client.fetch(self.url, method="STATS", allow_nonstandard_methods=True, body="", streaming_callback=self.handle_update, request_timeout=24*60*60, callback=self.handle_disconnect)

    def handle_disconnect(self, *args):
        """
        Handle a disconnect event, including triggering backoff

        @param args: Ignored
        """
        # Got a forced disconnect from the stats server
        print "Disconnected from stats server. Attempting reconnect in %d second(s)" % self.backoff
        self.connected = False
        tornado.ioloop.IOLoop.instance().add_timeout(self.backoff, self.connect)

        # Increase backoff
        self.backoff *= 2

    def handle_update(self, content):
        """
        Handle streaming update

        @param content: stream packet

        """
        # Successful data receive. Update backoff factor to 1s
        self.backoff = 1

        # First update since connecting?
        if not self.connected:
            print "Connection successful, updates running"
            self.connected = True

        # Because we can receive partial lines in the update
        # We add the current buffer to the content
        lines = (self.buffer + content).split("\n")
        # Then we take the last line (partial or empty) and
        # push it into the buffer for next request
        self.buffer = lines[-1]

        for line in lines[:-1]:
            # Remove any crap
            line = line.strip()

            # Output for debugging purposes
            if self.verbose:
                print line

            # Split up into values
            values = line.split(' ',3)

            # Less than 2 values and we don't know what's going on, ignore
            if len(values) < 2:
                continue

            self.action_handler.dispatch(*values)


class MainHandler(tornado.web.RequestHandler):
    """
    Any GET request will just give a JSON dump of the full set
    """
    def get(self, *args, **kwargs):
        """
        Handle GET request, writes out a JSON dump of the stream dataset

        @param args: Ignored
        @param kwargs: Ignored
        @return:
        """
        self.write(json.dumps(streams))

        
# Route / to Main
application = tornado.web.Application([
    (r"/", MainHandler),
])

# Set up server and start service request
if __name__ == "__main__":
    parser = OptionParser(usage="usage: %prog [options] http://url.to.stats/server")

    parser.add_option("--port","-p",
                      help = "Port to listen on",
                      type = "int",
                      default = 8888)
    parser.add_option("--verbose","-v",
                      help = "print event lines",
                      action = "store_true")

    (options, args) = parser.parse_args()

    if len(args) < 1:
        parser.print_help()
        sys.exit(1)

    server = tornado.httpserver.HTTPServer(application)
    server.listen(options.port)

    service = ServiceHandler(args[0], options.verbose)
    service.connect()
    tornado.ioloop.IOLoop.instance().start()