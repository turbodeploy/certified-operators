#!/bin/sh
echo turbonomic > /etc/hostname
yum install -y yum-utils
# Add network user
authconfig --disablemd5 --enableshadow --passalgo=sha512 --update
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

echo "" >> /etc/ssh/sshd_config
echo "# Turbonomic secure settings" >> /etc/ssh/sshd_config
echo "Ciphers aes128-cbc,aes128-ctr,aes192-cbc,aes192-ctr,aes256-cbc,aes256-ctr" >> /etc/ssh/sshd_config
echo "MACs hmac-sha2-256,hmac-sha2-512,hmac-sha1" >>/etc/ssh/sshd_config
echo "" >> /etc/ssh/sshd_config
echo "Banner /etc/ssh/vmtbanner" >> /etc/ssh/sshd_config
echo "" >> /etc/ssh/sshd_config

echo "Welcome to Turbonomic XL!" >/etc/ssh/vmtbanner

# Display useful information on the pre-login screen
echo "" >>/etc/rc.local
echo "# Turbonomic" >>/etc/rc.local
echo "echo \"Turbonomic XL\" >/etc/issue" >> /etc/rc.local
echo 'echo "Kernel: $(/bin/uname -r)" >>/etc/issue' >> /etc/rc.local
echo 'echo "IP: $(/sbin/ip route get 1 | /bin/head -n 1 | awk '"'"'{print $7}'"'"')" >>/etc/issue' >> /etc/rc.local
chmod +x /etc/rc.local
chmod +x /etc/rc.d/rc.local

# Set up permissions
chmod +x /etc/docker/turboctl.py
chmod +x /etc/docker/turboupgrade.py
chmod +x /usr/local/bin/vmtctl

# Add a symlink, so that the sudo would see it.
ln -s /etc/docker/turboctl.py /usr/bin/turboctl
ln -s /usr/local/bin/vmtctl /usr/bin/vmtctl

# clean up the disk
# Compress the image as much as possible
dd if=/dev/zero of=/mytempfile bs=1024  count=1
rm -rf /mytempfile

# Disable root user.
echo "root:$(dd if=/dev/urandom bs=1 count=32 2>/dev/null | base64 | tr '+' '_' | tr '/' '-' | head -c${1:-32};echo)" | chpasswd
usermod -s /bin/false root
passwd -l root
echo 'hostname: ' `hostname`
