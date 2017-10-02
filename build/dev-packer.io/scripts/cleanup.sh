#!/bin/sh
echo turbonomic > /etc/hostname
yum install -y yum-utils
# Add network user
chmod +x /opt/netedit
useradd -g wheel -d /home/neteditor -m -r -s /opt/netedit neteditor
echo "neteditor:vmturbo" | chpasswd
chage -d 0 neteditor

# Make sure this user may only execute a single command using sudo.
# Since that would be the only thing that user may execute, allow it without password.
# This all will be reverted after the proper host OS admin user has been created.
cp /etc/sudoers /opt/sudoers
cp /etc/sudoers /opt/sudoers.backup
chmod a+w /opt/sudoers
echo "neteditor ALL=NOPASSWD: /opt/netedit" >> /opt/sudoers
chmod 440 /opt/sudoers
cp /opt/sudoers /etc/sudoers

# Install python. We will need it for simple CGI-based HTTP server.
yum install -y python

# Disable root user.
#usermod -s /bin/false root
#passwd -l root
echo 'hostname: ' `hostname`

