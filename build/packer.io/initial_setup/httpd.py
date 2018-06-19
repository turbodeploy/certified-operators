#!/usr/bin/env python

__all__ = ["CGIHTTPRequestHandler"]

import os
import os.path
import socket
import BaseHTTPServer
import SimpleHTTPServer
import ssl
import thread
import time

class HTTP80RequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    # Request handler for http requests on port 80
    # Extracts the Host: from the headers and redirects the user to the
    # same host but on https port 443
    def do_GET(self):
        rbufsize = 0
        self.send_response(302)
        headers=str(self.headers)
        hostStr='Host:'
        connectionStr='Connection:'
        hostName=headers.split(hostStr)[-1].split(connectionStr)[0]
        self.send_header('Location',"https://"+hostName.strip())
        self.end_headers()

class CGIHTTPRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    # Make rfile unbuffered -- we need to read one line and then pass
    # the rest to a subprocess, so we can't use buffered input.
    rbufsize = 0

    def do_GET(self):
        # Exit
        if self.path == '/exit':
            self.send_response(200, "Script output follows")
            self.end_headers()
            self.wfile.write('Initialization complete')
            os._exit(0)
        # We proceed normally
        if self.path == '/status':
            self.send_response(200, "Script output follows")
            self.end_headers()
            if os.path.isfile('/tmp/load_status'):
                f = open('/tmp/load_status', 'r')
                status = f.read().strip()
                f.close()
            else:
                status = 'Initializing Turbonomic XL...'
            self.wfile.write(status)
        else:
            # Regular request
            f = self.send_head()
            if f:
                for line in f:
                    self.wfile.write(line)
                f.close()

    def do_POST(self):
        self.run_cgi()

    def log_request(self, code='-', size='-'):
        return 0

    cgi_directories = ['/cgi-bin', '/htbin']

    def run_cgi(self):
        thisHost = self.headers.get('Host')
        script = '/root/initial_setup/cgi-bin/add_user'
        scriptname = script
        content_len = int(self.headers.getheader('content-length', 0))
        query = self.rfile.read(content_len)

        # Unix -- fork as we should
        args = [script]
        queryargs = query.split("&")
        args.append(queryargs[0].split("=")[1])
        args.append(queryargs[1].split("=")[1])
        self.rfile.flush() # Always flush before forking
        self.wfile.flush() # Always flush before forking
        pid = os.fork()
        env = {}
        if pid != 0:
            # Parent
            pid, sts = os.waitpid(pid, 0)
            if sts:
                self.log_error("CGI script exit status %#x", sts)
            else:
                # Send the response
                self.send_response(200, "Script output follows")
                self.end_headers()

                # Open the file
                f = open('monitor.html', 'r')
                for line in f:
                    # Process special cases
                    if "var thisHost" in line:
                        line = line.replace(';', " = '%s';" % thisHost)
                    # Write the response line back.
                    self.wfile.write(line)
                f.close()
            return
        # Child
        try:
            os.execve(scriptname, args, env)
        except:
            self.server.handle_error(self.request, self.client_address)
            os._exit(127)


def executable(path):
    try:
        st = os.stat(path)
    except os.error:
        return 0
    return st[0] & 0111 != 0

def run(HandlerClass = CGIHTTPRequestHandler,
         ServerClass = BaseHTTPServer.HTTPServer):
	httpd = BaseHTTPServer.HTTPServer(('',443), CGIHTTPRequestHandler)
	httpd.socket = ssl.wrap_socket (httpd.socket,
					keyfile='/root/initial_setup/cgi-bin/cert/key.pem',
					certfile='/root/initial_setup/cgi-bin/cert/certificate.pem',
					ssl_version=ssl.PROTOCOL_TLSv1_2,
					server_side=True)
	httpd.serve_forever()

# Server listening on port 80 for http requests which will ultimately be redirected to https

def run80(HandlerClass = HTTP80RequestHandler,
         ServerClass = BaseHTTPServer.HTTPServer):
    httpd = BaseHTTPServer.HTTPServer(('',80), HTTP80RequestHandler)
    httpd.serve_forever()

# Starting two http server instances on different threads
# One instance listens on port 80 for http requests and redirects to https
if __name__ == '__main__':
    try:
        thread.start_new_thread(run,());
        thread.start_new_thread(run80,());
    except (KeyboardInterrupt, SystemExit):
        cleanup_stop_thread();
        sys.exit()

# Loop to ensure threas remain running
while 1:
    time.sleep(10)
