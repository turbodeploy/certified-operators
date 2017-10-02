#!/usr/bin/env python

__all__ = ["CGIHTTPRequestHandler"]

import os
import sys
import urllib
import BaseHTTPServer
import SimpleHTTPServer
import ssl

class CGIHTTPRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    # Make rfile unbuffered -- we need to read one line and then pass
    # the rest to a subprocess, so we can't use buffered input.
    rbufsize = 0

    def do_POST(self):
        if self.is_cgi():
            self.run_cgi()
        else:
            self.send_error(501, "Can only POST to CGI scripts")

    def send_head(self):
        if self.is_cgi():
            return self.run_cgi()
        else:
            return SimpleHTTPServer.SimpleHTTPRequestHandler.send_head(self)

    def is_cgi(self):
        path = self.path

        for x in self.cgi_directories:
            i = len(x)
            if path[:i] == x and (not path[i:] or path[i] == '/'):
                self.cgi_info = path[:i], path[i+1:]
                return 1
        return 0

    def log_request(self, code='-', size='-'):
        return 0

    cgi_directories = ['/cgi-bin', '/htbin']

    def run_cgi(self):
        dir, rest = self.cgi_info
        i = rest.rfind('?')
        if i >= 0:
            rest, query = rest[:i], rest[i+1:]
        else:
            query = ''
        i = rest.find('/')
        if i >= 0:
            script, rest = rest[:i], rest[i:]
        else:
            script, rest = rest, ''


        script = '/root/initial_setup/cgi-bin/setup.py'
        scriptname = script

        # Reference: http://hoohoo.ncsa.uiuc.edu/cgi/env.html
        # XXX Much of the following could be prepared ahead of time!
        env = {}
        env['SERVER_SOFTWARE'] = self.version_string()
        env['SERVER_NAME'] = self.server.server_name
        env['GATEWAY_INTERFACE'] = 'CGI/1.1'
        env['SERVER_PROTOCOL'] = self.protocol_version
        env['SERVER_PORT'] = str(self.server.server_port)
        env['REQUEST_METHOD'] = self.command
        uqrest = urllib.unquote(rest)
        env['PATH_INFO'] = uqrest
        env['PATH_TRANSLATED'] = self.translate_path(uqrest)
        env['SCRIPT_NAME'] = scriptname
        if query:
            env['QUERY_STRING'] = query
        host = self.address_string()
        if host != self.client_address[0]:
            env['REMOTE_HOST'] = host
        env['REMOTE_ADDR'] = self.client_address[0]
        # XXX AUTH_TYPE
        # XXX REMOTE_USER
        # XXX REMOTE_IDENT
        if self.headers.typeheader is None:
            env['CONTENT_TYPE'] = self.headers.type
        else:
            env['CONTENT_TYPE'] = self.headers.typeheader
        length = self.headers.getheader('content-length')
        if length:
            env['CONTENT_LENGTH'] = length
        accept = []
        for line in self.headers.getallmatchingheaders('accept'):
            if line[:1] in "\t\n\r ":
                accept.append(line.strip())
            else:
                accept = accept + line[7:].split(',')
        env['HTTP_ACCEPT'] = ','.join(accept)
        ua = self.headers.getheader('user-agent')
        if ua:
            env['HTTP_USER_AGENT'] = ua
        co = filter(None, self.headers.getheaders('cookie'))
        if co:
            env['HTTP_COOKIE'] = ', '.join(co)

        decoded_query = query.replace('+', ' ')

        # Unix -- fork as we should
        args = [script]
        if '=' not in decoded_query:
            args.append(decoded_query)
        self.rfile.flush() # Always flush before forking
        self.wfile.flush() # Always flush before forking
        pid = os.fork()
        if pid != 0:
            # Parent
            pid, sts = os.waitpid(pid, 0)
            if sts:
                self.log_error("CGI script exit status %#x", sts)
            else:
                self.send_response(200, "Script output follows")
                self.end_headers()
                self.wfile.write('<html>')
                self.wfile.write('<script type="text/javascript">')
                self.wfile.write('window.location.href = "http://www.turbonomic.com"')
                self.wfile.write('</script>')
                self.wfile.write('<body>onload="redir()"</body>')
                self.wfile.write('</html>')
                os._exit(0)
            return
        # Child
        try:
            os.dup2(self.rfile.fileno(), 0)
            os.dup2(self.wfile.fileno(), 1)
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


if __name__ == '__main__':
    run()

