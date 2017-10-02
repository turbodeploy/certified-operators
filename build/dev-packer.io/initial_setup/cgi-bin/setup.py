#!/usr/bin/env python

import cgi, subprocess, sys

form = cgi.FieldStorage()

## getting the data from the fields
user = form.getvalue('username')
password = form.getvalue('password')
cmd = "/root/initial_setup/cgi-bin/add_user " + user + " " + password

ret = subprocess.call(cmd, shell=True)
sys.exit(ret)
