#!/usr/bin/perl -w
#
# test.cgi -- A CGI script for FileUpload module demonstration.
#	      Make sure FileUpload.pm has been installed before
#	      you start to test this script.
#	      This script reads input from "./form.html" file,
#	      it expects there is a file being uploaded with
#	      input field name, ufile.
#
# Simon Tneoh Chee-Boon tneohcb@pc.jaring.my
# 
# Copyright (c) 2000-2002 Simon Tneoh Chee-Boon. All rights reserved.
# This program is free software; you can redistribute it and/or
# modify it under the same terms as Perl itself.
#
use strict;
use vars qw($cgi $fu $filename $sizewritten $rpmresult @fileholder);
use VMTurbo;
use Getopt::Std;

# Autoflush.
$| = 1;

$VMTurbo::debug = 0;
my $server = 'HTTP://guest:guest@localhost:8080';

my $s = VMTurbo::Session->new( $server );
die "Cant connect to $server" unless $s;

# Print the HTTP header line.
# I am not using CGI->header here because I want to check if CGI
# version 2.46 or above is installed.

# Make sure CGI version 2.46 or above is installed.
eval("use CGI 2.46 qw(-private_tempfiles :standard);");
(print("Error: $@"), exit(-1)) if $@;

# Make sure FileUpload is installed.
eval("use FileUpload 0.07;");
(print("Error: $@"), exit(-1)) if $@;

# Create CGI object.
$cgi = new CGI;

# Has the action been specified?
if (! $cgi->param('action')) {
  print "Content-Type: text/html\n\n";
  print <<__END__ERROR__;
<html>
<head><title>Error</title></head>
<body bgcolor='white'>
It seems like you have not selected any an action.
Please try again.
</body>
</html>
__END__ERROR__
  exit(-1);
}

my $ses = $s->entity( "LoginService", undef );
my $authenticateresult = $ses->authenticate($cgi->param('username'), $cgi->param('userpassword'));
my $viewconfig = $ses->findViewConfig($cgi->param('username'));
$s->close();

if ($authenticateresult == 0) {
    print "Content-Type: text/html\n\n";
	print "Authentication failed!\n";
	exit(-1);
}

if ( $viewconfig ne "view-administrator.config.topology") {
    print "Content-Type: text/html\n\n";
	print "User not allowed to update!\n";
	exit(-1);
}

if ($cgi->param('action') eq "About") {
	$rpmresult = `/srv/tomcat6/script/appliance/vmtupdate.sh -o about; cat /tmp/vmturbo_about.txt`; 
    print "Content-Type: text/html\n\n";
	print "<pre>"; print $rpmresult;  print "</pre>";
}

if ($cgi->param('action') eq "Check") {
	$rpmresult = `/srv/tomcat6/script/appliance/vmtupdate.sh -o check; cat /tmp/vmturbo_check.txt`; 
    print "Content-Type: text/html\n\n";
	print "<pre>"; print $rpmresult;  print "</pre>";
}

if ($cgi->param('action') eq "Update") {
	$rpmresult = `/srv/tomcat6/script/appliance/vmtupdate.sh -o update; cat /tmp/vmturbo_update.txt`; 
    print "Content-Type: text/html\n\n";
	print "<pre>"; print $rpmresult;  print "</pre>";
}

if ($cgi->param('action') eq "Upload Branding") {
# Has any files been uploaded?
if (! $cgi->param('ufile')) {
  print "Content-Type: text/html\n\n";
  print <<__END__ERROR__;
<html>
<head><title>Error</title></head>
<body bgcolor='white'>
It seems like you have not selected any files yet.
Please try again.
</body>
</html>
__END__ERROR__
  exit(-1);
}

# Create the FileUpload object.
$fu = new FileUpload($cgi->param('ufile'));
$filename=$fu->orig_filename;
chdir "/tmp";

if (! $fu) {
  print "Content-Type: text/html\n\n";
  print <<__END__ERROR__;
<html>
<head><title>Error</title></head>
<body bgcolor='white'>
Failed to create FileUpload object.
Please try again.
</body>
</html>
__END__ERROR__
}

# Try to save the uploaded file.
$sizewritten = -1;
if (!($sizewritten = $fu->save)) {
  print "Content-Type: text/html\n\n";
  print "Failed to upload file to $filename $!.\n";
}
else {
  print "Content-Type: text/html\n\n";
  print "File uploaded to $filename with $sizewritten bytes.\n";
  $rpmresult = `cd /; sudo /usr/bin/unzip -q -o /tmp/$filename`; 
  print "<pre>"; print $rpmresult;  print "</pre>";
}
}

if ($cgi->param('action') eq "Upload Integration") {
# Has any files been uploaded?
if (! $cgi->param('ufile')) {
  print "Content-Type: text/html\n\n";
  print <<__END__ERROR__;
<html>
<head><title>Error</title></head>
<body bgcolor='white'>
It seems like you have not selected any files yet.
Please try again.
</body>
</html>
__END__ERROR__
  exit(-1);
}

# Create the FileUpload object.
$fu = new FileUpload($cgi->param('ufile'));
$filename=$fu->orig_filename;
chdir "/tmp";

if (! $fu) {
  print "Content-Type: text/html\n\n";
  print <<__END__ERROR__;
<html>
<head><title>Error</title></head>
<body bgcolor='white'>
Failed to create FileUpload object.
Please try again.
</body>
</html>
__END__ERROR__
}

# Try to save the uploaded file.
$sizewritten = -1;
if (!($sizewritten = $fu->save)) {
  print "Content-Type: text/html\n\n";
  print "Failed to upload file to $filename $!.\n";
}
else {
  print "Content-Type: text/html\n\n";
  print "File uploaded to $filename with $sizewritten bytes.\n";
  $rpmresult = `rm -rf /tmp/control; mkdir /tmp/control; cd /tmp/control; unzip -q /tmp/$filename; chmod 755 /tmp/control/*; sudo cp /tmp/control/* /srv/tomcat6/script/control/`; 
  print "<pre>"; print $rpmresult;  print "</pre>";
}
}

if ($cgi->param('action') eq "Download Branding") {
  system("rm /tmp/branding.zip; cd /; zip /tmp/branding.zip /srv/www/htdocs/index.html /srv/www/htdocs/com.vmturbo.UI/UIMain.html /srv/www/htdocs/com.vmturbo.UI/assets/images/logo-vmturbo.jpg /srv/reports/images/logo.png /srv/reports/images/copyright.jpg /srv/rails/webapps/persistence/public/images/VMTurbo.jpg /srv/rails/webapps/persistence/public/images/vmt_logo.jpg");
  open(DLFILE, "</tmp/branding.zip");
  @fileholder = <DLFILE>;
  close (DLFILE);
  print "Content-Type:application/x-download\n";
  print "Content-Disposition:attachment;filename=branding.zip\n\n";
  print @fileholder;
}

if ($cgi->param('action') eq "Download Integration") {
  system("rm /tmp/action_scripts.zip; cd /srv/tomcat6/script/control; zip /tmp/action_scripts.zip *.sh");
  open(DLFILE, "</tmp/action_scripts.zip");
  @fileholder = <DLFILE>;
  close (DLFILE);
  print "Content-Type:application/x-download\n";
  print "Content-Disposition:attachment;filename=action_scripts.zip\n\n";
  print @fileholder;
}

if ($cgi->param('action') eq "Upload and update") {
# Has any files been uploaded?
if (! $cgi->param('ufile')) {
  print "Content-Type: text/html\n\n";
  print <<__END__ERROR__;
<html>
<head><title>Error</title></head>
<body bgcolor='white'>
It seems like you have not selected any files yet.
Please try again.
</body>
</html>
__END__ERROR__
  exit(-1);
}

# Create the FileUpload object.
$fu = new FileUpload($cgi->param('ufile'));
$filename=$fu->orig_filename;
chdir "/tmp";

if (! $fu) {
  print "Content-Type: text/html\n\n";
  print <<__END__ERROR__;
<html>
<head><title>Error</title></head>
<body bgcolor='white'>
Failed to create FileUpload object.
Please try again.
</body>
</html>
__END__ERROR__
}

# Try to save the uploaded file.
$sizewritten = -1;
if (!($sizewritten = $fu->save)) {
  print "Content-Type: text/html\n\n";
  print "Failed to upload file to $filename $!.\n";
}
else {
  print "Content-Type: text/html\n\n";
  print "File uploaded to $filename with $sizewritten bytes.\n";
  system("cd /tmp; rm -rf /tmp/*.rpm /tmp/vmturbo; unzip -q /tmp/$filename");
	if ( -d "/tmp/vmturbo") {
		if ( -f "/etc/SuSE-release" ) {
			$rpmresult = `sudo /usr/bin/zypper ar -f /tmp/vmturbo vmturbo_temp; sudo /usr/bin/zypper --non-interactive --no-gpg-checks up -r vmturbo_temp; sudo /usr/bin/zypper rr vmturbo_temp`;
		} elsif ( -f "/etc/redhat-release" ) {
			$rpmresult = `sudo cp /tmp/vmturbo_temp.repo /etc/yum.repos.d/; sudo /usr/bin/yum -y update vmt-bundle vmt-config vmt-persistence vmt-platform vmt-presentation vmt-reports birt-runtime`;
		}
	} else {
		$rpmresult = `sudo /bin/rpm -v -U /tmp/*.rpm`;
	}
  print "<pre>"; print $rpmresult;  print "</pre>";
}
}

# Done.
exit(0);
