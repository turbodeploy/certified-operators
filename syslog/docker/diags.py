#!/usr/bin/env python2.7

__all__ = ["CGIHTTPRequestHandler"]

import os
import os.path
import BaseHTTPServer
import SimpleHTTPServer

class CGIHTTPRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    rbufsize = 0
    wbufsize = 1024

    def do_GET(self):
        # Exit
        if self.path == '/diagnostics':
            self.run_cgi()
        if self.path == '/proactive':
            self.run_cgi_proactive()

    def log_request(self, code='-', size='-'):
        return 0

    cgi_directories = ['/cgi-bin', '/htbin']

    def run_cgi_proactive(self):
        script = '/collect_diags_proactive.sh'
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
            try:
                # Always read in binary mode. Opening files in text mode may cause
                # newline translations, making the actual size of the content
                # transmitted *less* than the content-length!
                f = open("/home/vmtsyslog/tmp/rsyslog_proactive.tar.gz", 'rb')
                self.send_response(200)
                self.send_header("Content-type", "application/octet-stream")
                fs = os.fstat(f.fileno())
                self.send_header("Content-Length", str(fs[6]))
                self.send_header("Last-Modified", self.date_time_string(fs.st_mtime))
                self.end_headers()
                self.copyfile(f, self.wfile)
                f.close()
            except:
                self.log_error("Error sending response")
            return
        # Child
        try:
            os.execve(scriptname, args, env)
        except:
            self.log_error("Error running script")

    def run_cgi(self):
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
            try:
                # Always read in binary mode. Opening files in text mode may cause
                # newline translations, making the actual size of the content
                # transmitted *less* than the content-length!
                f = open("/home/vmtsyslog/tmp/rsyslog.zip", 'rb')
                self.send_response(200)
                self.send_header("Content-type", "application/zip")
                fs = os.fstat(f.fileno())
                self.send_header("Content-Length", str(fs[6]))
                self.send_header("Last-Modified", self.date_time_string(fs.st_mtime))
                self.end_headers()
                self.copyfile(f, self.wfile)
                f.close()
            except:
                self.log_error("Error sending response")
            return
        # Child
        try:
            os.execve(scriptname, args, env)
        except:
            self.log_error("Error running script")

def executable(path):
    try:
        st = os.stat(path)
    except os.error:
        return 0
    return st[0] & 0111 != 0

# The loopback interface is a class A network, so the address below is a loopback address.
def run(HandlerClass = CGIHTTPRequestHandler,
        ServerClass = BaseHTTPServer.HTTPServer):
    httpd = BaseHTTPServer.HTTPServer(('',8080), CGIHTTPRequestHandler)
    httpd.serve_forever()

if __name__ == '__main__':
    run()

