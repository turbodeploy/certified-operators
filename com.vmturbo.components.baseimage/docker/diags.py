#!/usr/bin/env python2.7

__all__ = ["CGIHTTPRequestHandler"]

import os
import os.path
import BaseHTTPServer
import SimpleHTTPServer

class CGIHTTPRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    rbufsize = 0

    def do_GET(self):
        # Exit
        if self.path == '/diags':
            self.run_cgi()

    def log_request(self, code='-', size='-'):
        return 0

    cgi_directories = ['/cgi-bin', '/htbin']

    def run_cgi(self):
        # Send the response right away
        # We do not want to wait for what is potentially a very long operation.
        self.send_response(200, "Done")
        self.end_headers()

        script = '/collect_diags.sh'
        scriptname = script

        # Unix -- fork as we should
        args = [script]
        self.rfile.flush() # Always flush before forking
        self.wfile.flush() # Always flush before forking
        pid = os.fork()
        env = {}
        if pid != 0:
            # Parent
            pid, sts = os.waitpid(pid, 0)
            if sts:
                self.log_error("CGI script exit status %#x", sts)
            return
        # Child
        try:
            os.execve(scriptname, args, env)
        except:
            self.server.handle_error(self.request, self.client_address)

def executable(path):
    try:
        st = os.stat(path)
    except os.error:
        return 0
    return st[0] & 0111 != 0

# The loopback interface is a class A network, so the address below is a loopback address.
def run(HandlerClass = CGIHTTPRequestHandler,
        ServerClass = BaseHTTPServer.HTTPServer):
    httpd = BaseHTTPServer.HTTPServer(('127.128.129.130',58888), CGIHTTPRequestHandler)
    httpd.serve_forever()


if __name__ == '__main__':
    run()

