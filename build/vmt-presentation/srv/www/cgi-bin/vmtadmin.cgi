#!/usr/bin/perl
# Script to handle VMTurbo updates from new HTML 5 uploader routine
# Alec Kemp VMTurbo 2012
# Script is called 3 ways, defined by CGI param calltype
# ACTION - do something
# READ - send back the current contents of log file (this happens repeatedly while ACTION is
# completing on another XHR request)
# DOWN - send back the contents of a file (zip etc.) that has been requested. This happens after
# ACTION has completed (which creates the file).
# each 'way' has multiple types of behavior, controlled by actiontype CGI param e.g. upload branding, update vmturbo, get diags file etc

#use strict;
#use Time::gmtime;
#use warnings;

# Force a flush right away and after every write or print on the currently
# selected output channel.
$| = 1;

use CGI qw ( -unique_headers );
# Need to use IO as standard perl file handling seems to lock exclusive
use IO::File qw();
# Suppress redundant HTTP Headers
$CGI::HEADERS_ONCE = 1;
use vars qw($rpmresult);
# Uncomment for CGI fatals in browser
#use CGI::Carp qw ( fatalsToBrowser );
use CGI::Carp qw ();
use File::Basename;
use VMTurbo;
use threads;


# Set Error message for Carp fatals - create a custom handler

BEGIN {
	sub carp_error {
		my $error_message = shift;
		&error( $error_message );
	}
	CGI::Carp::set_message( \&carp_error );
}

sub error {
	my( $error_message ) = @_;
	print "<CENTER><h2>Oh No!</h2></CENTER>";
	print "<p>Got an error:</p>";
	print "<p>".$error_message."</p>";
	exit;
}

# Set max file size limit here - currently 500MB. File will be rejected post-upload if over this size.
# The JS should also enforce this pre-upload, but as client code it cannot be trusted.
# param() returns empty set if this is triggered
$CGI::POST_MAX = 1024 * 500000;


# Set up CGI for form data
my $query = new CGI;

# Characters that are safe in values that are sent to a shell.	We don't allow
# any kind of quoting.	Since / isn't allowed, file paths are not allowed,
# only simple file names.  Space characters are not allowed as they may
# terminated arguments.	 Beware of leading "-" or ".." - they need further
# processing.
my $safe_for_shell = "-a-zA-Z0-9_+=@.,:";

# Set up variables from calling form
my $vmtuser = $query->param("userName");
my $vmtuuid = $query->param("userUUID");
my $vmtpassword = $query->param("password");
# What the CGI should do.  This is compared to pre-defined strings, which
# is safe - but it's also used to construct a default status file name.  No
# harm checking it.
my $actiontype = &getSafeParam("actionType");
# Leading part of constructed output file name.	 The name is historical/
# conventional:	 If not supplied as a parameter, the date/time is used.
my $filedate = &getSafeParam("fileDate");
$filedate =~ s/ /_/g;
# Action or output read flag
# calltype is ACTION for doing, READ for the output text, and DOWN for file
# stream download
my $calltype = $query->param("callType");
# Proxy and SMTP settings
my $proxyState = &getSafeParam("proxyState");
my $proxyHost = &getSafeParam("proxyHost");
my $proxyUser = &getSafeParam("proxyUser");
my $smtpState = &getSafeParam("smtpState");
my $smtpHost = &getSafeParam("smtpHost");
my $rpmToUpdate = &getSafeParam("rpmToUpdate");

# Directory to which any uploaded files will go.  It's also used for temporary
# files we will download.
my $uploadDir = "/tmp/";

# Full path to an uploaded file.  It will always be in $uploadDir.
my $uploadedTo;

# Construct filename to send back progress from
my $statusFile;
if (defined $filedate) {
  $statusFile = "$filedate.$actiontype.vmturbo.txt";
} else {
  my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);

  # Convert Month and Year values into human-readable dates
  # as month is zero based and year is 'years since 1900' by default)
  $mon += 1;
  $year += 1900;

  $statusFile = "$mon.$mday.$year.$hour.$min.$actiontype.vmturbo.txt";
}
$statusFile = $uploadDir.$statusFile;

if ( $calltype eq "READ" ) {
	# Send back output file contents
	print $query->header(-type => "text/plain");
	my $statushandle = IO::File->new($statusFile, 'r');
	die "Could not read $statusFile: $!" unless $statushandle;
	while (defined (my $line = $statushandle->getline)) {
		print($line);
	}
	$statushandle->close();
	exit();
}
elsif ($calltype eq "ACTION" ) {
	# Carry out action
	# Every run has a VERSIONS and AUTHENTICATE
	# plus another actiontype
	if ( $actiontype eq "VERSIONS" ) {
		# Initial call, to populate version information popup
		&versionInfo;
		exit();
	}

	# Every action needs to be authenticated
	&authenticate;

	if ( $actiontype eq "AUTHENTICATE" ) {
		# Initial user check, and also called with each action,
		# in case user modifies username password after initial
		# authentication. Client-side JS should prevent this
		# but cannot be trusted.
		print $query->header();
		print "SUCCESS";
		exit();
	}

	# NTP check
	if ( $actiontype eq "NTPCHECK" ) {
		# If we get this far, authentication is good
		&ntpCheck;
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# process check
	if ( $actiontype eq "PROCESSCHECK" ) {
		# If we get this far, authentication is good
		&processCheck;
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# create file for output progress
	$outhandle = IO::File->new($statusFile, 'a');
	die "Could not append to $statusFile: $!" unless $outhandle;
	$outhandle->autoflush(1);

	# This is a check for update
	if ( $actiontype eq "CHECK" ) {
		# If we get this far, authentication is good
		$check = &checkVMT;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
        print $check;
		exit();
	}

	# This is an offline update
	if ( $actiontype eq "UPDATE" ) {
		# If we get this far, authentication is good
		&doUpload;
		&updateVMT;
		$outhandle->close();
		# The one in /tmp will be deleted next time page is loaded
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# This is an online update
	if ( $actiontype eq "UPTODATE" ) {
		# If we get this far, authentication is good
		&uptodateVMT;
		$outhandle->close();
		# The one in /tmp will be deleted next time page is loaded
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# This is a branding update
	if ( $actiontype eq "BRANDING" ) {
		# If we get this far, authentication is good
		&doUpload;
		&updateBranding;
		$outhandle->close();
		# The one in /tmp will be deleted next time page is loaded
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# This is an integration pack upload
	if ( $actiontype eq "INTEGRATE" ) {
		# If we get this far, authentication is good
		&doUpload;
		&updateIntegration;
		$outhandle->close();
		# The one in /tmp will be deleted next time page is loaded
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# Expert mode - upload file to $uploadDir
	if ( $actiontype eq "EXPERTFILE" ) {
		# If we get this far, authentication is good
		&doUpload;
		# Close output progress file
		$outhandle->close();
		# Send header to prevent HTTP500
		print $query->header();
		exit();
	}

	# This is a branding download
	if ( $actiontype eq "GETBRAND" ) {
		# If we get this far, authentication is good
		&getBranding;
		# File has been created. Seperate CGI call will retrieve.
		$outhandle->close();
		# Send Minimal header to prevent HTTP500
		print $query->header();
		exit();
	}

	# This is an integration download
	if ( $actiontype eq "GETINTEGRATE" ) {
		# If we get this far, authentication is good
		&getIntegrate;
		$outhandle->close();
		# Send Minimal header to prevent HTTP500
		print $query->header();
		exit();
	}

	# Get Proxy information
	if ( $actiontype eq "GETPROXYCONFIG" ) {
		# If we get this far, authentication is good
		&getProxyConfig;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# Set Proxy information
	if ( $actiontype eq "SETPROXYCONFIG" ) {
		# If we get this far, authentication is good
		&setProxyConfig;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# Get SMTP information
	if ( $actiontype eq "GETSMTPCONFIG" ) {
		# If we get this far, authentication is good
		&getSMTPConfig;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# Set SMTP information
	if ( $actiontype eq "SETSMTPCONFIG" ) {
		# If we get this far, authentication is good
		&setSMTPConfig;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# Full backup to /tmp
	if ( $actiontype eq "FULLBACKUP" ) {
		# If we get this far, authentication is good
		$outhandle->say("Looking for diagnostics file..");
		&fullBackup;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# Expert mode - export backup from /tmp
	if ( $actiontype eq "CFGBACKUP" ) {
		# Cfg backup to /tmp
		# If we get this far, authentication is good
		$outhandle->say("Looking for diagnostics file..");
		&cfgBackup;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# Expert mode - export backup from /tmp
	if ( $actiontype eq "EXPORTBACKUP" ) {
		# If we get this far, authentication is good
		$outhandle->say("Looking for diagnostics file..");
		&exportBackup;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# Restore backup from /tmp
	if ( $actiontype eq "FULLRESTORE" ) {
		# If we get this far, authentication is good
		$outhandle->say("Looking for backup file..");
		&doUpload;
		&fullRestore;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# Restore backup from /tmp
	if ( $actiontype eq "CFGRESTORE" ) {
		# If we get this far, authentication is good
		$outhandle->say("Looking for backup file..");
		&doUpload;
		&cfgRestore;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# Expert mode - get last bkp file from /tmp
	if ( $actiontype eq "EXPERTDIAGS" ) {
		# If we get this far, authentication is good
		$outhandle->say("Looking for diagnostics file..");
		&checkDiags;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
		exit();
	}

	# Online Diags Export
	if ( $actiontype eq "EXPORTDIAGS" ) {
		# If we get this far, authentication is good
		&exportDiags;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
		exit();
	}
}
elsif ( $calltype eq "DOWN" ) {

	# Every download needs to be authenticated
	&authenticate;
	# Send back contents of file we created
	if ($actiontype eq "GETBRAND" ) {
		# Download of branding zip file
		# send the branding file back
		# TODO: Check if missing (should not be possible, as we just created it)
		open(DLFILE, "</tmp/branding.zip");
		@fileholder = <DLFILE>;
		close (DLFILE);
		# remove the log file as we dont need it
		# Allow at least one read of the logs first
		sleep(1);
		system("rm \"$statusFile\"");
		#print "Content-Type:application/x-download\n";
		#MIME type set correctly will follow user prefs
		print "Content-Type:application/zip\n";
		print "Content-Disposition:attachment;filename=branding.zip\n\n";
		print @fileholder;
	}
	if ( $actiontype eq "GETINTEGRATE" ) {
		# This is an integration download
		# TODO: Check if missing (should not be possible, as we just created it)
		open(DLFILE, "</tmp/action_scripts.zip");
		@fileholder = <DLFILE>;
		close (DLFILE);
		# remove the log file as we dont need it
		# Allow at least one read of the logs first
		sleep(1);
		system("rm \"$statusFile\"");
		print "Content-Type:application/zip\n";
		print "Content-Disposition:attachment;filename=action_scripts.zip\n\n";
		print @fileholder;
	}
	if ( $actiontype eq "FULLBACKUP" ) {
		# Full backup download
		# TODO: Check if missing (should not be possible, as we just created it)
		open(DLFILE, "</tmp/vmtbackup.zip");
		@fileholder = <DLFILE>;
		close (DLFILE);
		# remove the log file as we dont need it
		# Allow at least one read of the logs first
		sleep(1);
		system("rm \"$statusFile\"");
		print "Content-Type:application/zip\n";
		print "Content-Disposition:attachment;filename=vmtbackup.zip\n\n";
		print @fileholder;
	}

	if ( $actiontype eq "CFGBACKUP" ) {
		# Cfg download
		# TODO: Check if missing (should not be possible, as we just created it)
		open(DLFILE, "</tmp/vmtbackup.zip");
		@fileholder = <DLFILE>;
		close (DLFILE);
		# remove the log file as we dont need it
		# Allow at least one read of the logs first
		sleep(1);
		system("rm \"$statusFile\"");
		print "Content-Type:application/zip\n";
		print "Content-Disposition:attachment;filename=vmtbackup.zip\n\n";
		print @fileholder;
	}
	if ( $actiontype eq "EXPORTBACKUP" ) {
		# Expert mode - get last bkp file from /tmp
		$fileToSend = `ls -t /tmp/vmtbackup.zip 2>/dev/null | head -1`;
		if ( length $fileToSend != 0 ) {
			# We found something
			chomp($fileToSend);
			$filebase = basename( $fileToSend );
			open(DLFILE, $fileToSend);
			@fileholder = <DLFILE>;
			close (DLFILE);
			# remove the log file as we dont need it
			# Allow at least one read of the logs first
			sleep(1);
			system("rm \"$statusFile\"");
			$filebase = basename( $fileToSend );
			print "Content-Type:application/zip\n";
			print "Content-Disposition:attachment;filename=".$filebase."\n\n";
			print @fileholder;
		} else {
			# remove the log file as we dont need it
			#Allow at least one read of the logs first
			sleep(1);
			system("rm \"$statusFile\"");
			# Prevent HTTP 500
			print $query->header();
			exit();
		}
	}
	if ( $actiontype eq "EXPERTDIAGS" ) {
		# Expert mode - get last diag file from /tmp
		$fileToSend = `ls -t /tmp/bkp-*.zip 2>/dev/null | head -1`;
		if ( length $fileToSend != 0 ) {
			# We found something
			chomp($fileToSend);
			$filebase = basename( $fileToSend );
			open(DLFILE, $fileToSend);
			@fileholder = <DLFILE>;
			close (DLFILE);
			# remove the log file as we dont need it
			# Allow at least one read of the logs first
			sleep(1);
			system("rm \"$statusFile\"");
			$filebase = basename( $fileToSend );
			print "Content-Type:application/zip\n";
			print "Content-Disposition:attachment;filename=".$filebase."\n\n";
			print @fileholder;
		} else {
			# remove the log file as we dont need it
			#Allow at least one read of the logs first
			sleep(1);
			system("rm \"$statusFile\"");
			# Prevent HTTP 500
			print $query->header();
			exit();
		}
	}
	if ( $actiontype eq "EXPORTDIAGS" ) {
		# Online Diags download
		# Expert mode - get last bkp file from /tmp
		$fileToSend = `ls -t /tmp/bkp-*.zip 2>/dev/null | head -1`;
		if ( length $fileToSend != 0 ) {
			# We found something
			chomp($fileToSend);
			$filebase = basename( $fileToSend );
			open(DLFILE, $fileToSend);
			@fileholder = <DLFILE>;
			close (DLFILE);
			# remove the log file as we dont need it
			# Allow at least one read of the logs first
			sleep(1);
			system("rm \"$statusFile\"");
			$filebase = basename( $fileToSend );
			print "Content-Type:application/zip\n";
			print "Content-Disposition:attachment;filename=".$filebase."\n\n";
			print @fileholder;
		} else {
			# remove the log file as we dont need it
			#Allow at least one read of the logs first
			sleep(1);
			system("rm \"$statusFile\"");
			# Prevent HTTP 500
			print $query->header();
			exit();
		}
	}
}
elsif (!$query->param()) {
	# The file has exceeded POST_MAX
	#print $query->header();
	#print "Sorry, this file is too large to upload.";
	die("File is too big");
}

# Only arrive here if unrecognised call type
# This should never happen (unless user edits jscript)
print $query->header();
print "Unrecognised Call Type! - Try reloading the page";
# Uncomment for debug.	Don't leave uncommented as it opens cross-site-
# scripting attacks!
#print $data;
exit(-1);

# Retrieve the value of a parameter that is safe to use in a shell command
# line.	 Argument is the name of the parameter; the value is returned if safe;
# the script terminates with an error otherwise.
sub getSafeParam
{
  my $paramName = shift;
  my $param = $query->param($paramName);
  return (defined $param) ? &checkParam($paramName, $param) : $param;
}

# Check a parameter for safety for use as in a shell command line.  Call
# as &checkParam($name, $value).  The first parameter is a name used only in
# error messages.  Returns $value if it's safe; otherwise, calls die.
sub checkParam
  {
  my $name = shift;
  my $param = shift;
  unless ($param =~ /^[$safe_for_shell]+$/) {
    die "Parameter $name contains invalid characters";
  }

  # A leading .. can be dangerous if used after, say, /tmp/.
  if ($param =~ /^\.\./) {
    die "Parameter $name cannot begin with two successive periods"
  }

  # A leading hyphen looks like a switch which can cause problems.
  if ($param =~ /^-/) {
    die "Parameter $name cannot begin with hyphen"
  }

  return $param;
}

sub outputLine
{
	# Print a line out to the current outputfile
	# Can be HTML too, this will render in browser
	# but will make log less readable
	# newlines are converted to br by jscript
	# tagged with UTC date time
	my( $outline ) = @_;
	$outhandle->say($outline." ".gmtime()." UTC");
}

sub versionInfo
{
	# Get version information from OS and VMT
	# Called once at page load
	# Passes back text formatted results
	# System line below will clean up /tmp from old update log files
	# For now, keep them in /tmp since we are not saving them anywhere else
	#system("rm -f /tmp/*.vmturbo.txt");
	# Get environment information
	# Determine 32 or 64 bit OS and remove whitespace
	$OStype = `getconf LONG_BIT`;
	$OStype = join(' ',split(' ',$OStype));

	# Get appliance version, and bit
	$VMT_full_version = "Unknown";
	$VMT_full_version = `rpm -q vmt-config`;

	# expected output format for rpm command contained in VMT_full_version
	# vmt-config-a.b-12345.i586 = 32 bit
	# vmt-config-a.b-12345.x86_64 = 64 bit
	# note, a.b version can also have sub versions a.b.c or a.b.c.d etc
	@VMT_build_split = split (/\-/, $VMT_full_version);
	$vmt_release = $VMT_build_split[2];
	$vmt_build_array_size = $#VMT_build_split;
	$vmt_build_type_end = $VMT_build_split[ $vmt_build_array_size ];
	@vmt_build_number = split (/\./, $vmt_build_type_end );
	$vmt_version = $vmt_build_number[0];
	$vmt_type = $vmt_build_number[1];
	#get rid of whitespace
	$vmt_type = join(' ',split(' ',$vmt_type));
	$vmt_friendly_type = "Unknown";
	if ( $vmt_type eq "i586" ) {
		$vmt_friendly_type = "32";
	}
	elsif ( $vmt_type eq "x86_64" ) {
		$vmt_friendly_type = "64";
	}
	# Check if the user is allowed to update their appliance
	# and return this information with the version info.
        my $rc = &checkAllowUpdate();
	print $query->header(-type => "text/plain");
	print "vmtbuild:".$vmt_version.",vmtrelease:".$vmt_release.",vmtbits:".$vmt_friendly_type.",osbits:".$OStype.",updateAllowed:".$rc;
}


sub authenticate
{
	# Set up VMTurbo specific connections
	$VMTurbo::debug = 0;
	my $server = 'HTTP://guest:guest@localhost:8080';
	my $s = VMTurbo::Session->new( $server );
	die "Can't connect to $server" unless $s;
	# Check the VMTurbo user specifed exists, and is allowed to update (admin user)
	my $ses = $s->entity( "LoginService", undef );
	my $authenticateresult = -1;
	if ($vmtpassword ne "") {
		$authenticateresult = $ses->authenticate($vmtuser, $vmtpassword);
	}
	elsif ($vmtuuid ne "") {
		$authenticateresult = $ses->authenticateByCookie($vmtuser, $vmtuuid);
	}
	my $viewconfig = $ses->findViewConfig($vmtuser);
	$s->close();
	if ($authenticateresult != 1) {
		print $query->header();
		print "FAIL";
		exit(-1);
	}
	if ( $viewconfig ne "view-administrator.config.topology") {
		print $query->header();
		print "NOPRIV";
		exit(-1);
	}
}

# Upload a file.  (Keep in mind that we are running on the server, so an
# uploaded file is one we *receive*.)  $uploadedTo is set to a computed path
# to which the uploaded data goes.
sub doUpload
{
	my $fileToUpload = $query->param("fileToUpload");
	unless ($fileToUpload) {
		print $query->header();
		die "There was a problem uploading the update file - the CGI did not get the filename!";
	}

	# Construct the full path to which the file will be uploaded.  It's
	# constructed by taking the fileToUpload parameter and converting it
	# into a "safe" file name - one that can be used safely on a shell
	# command line - and pre-pending $uploadDir.  To do the conversion, we
	# simply replace every character not in $safe_for_shell with an "_";
	# then changing a leading ".." to "_.." so that it's impossible to
	# "escape up" out of $uploadDir.  (Leading - is harmless as it will be
	# inside the path.)
	$uploadedTo = $fileToUpload;
	$uploadedTo =~ s/[^$safe_for_shell]/_/g;
	$uploadedTo =~ s/^\.\./_../;
	$uploadedTo = $uploadDir.$uploadedTo;
	open(UPLOADFILE, ">$uploadedTo")
	  or die "Unable to to create file '$uploadedTo'.  The error was $!";
	binmode UPLOADFILE;

	&outputLine("Upload processing to $uploadedTo started at");
	my $upload_filehandle = $query->upload("fileToUpload");
	if (!$upload_filehandle && $query->cgi_error) {
	  die "Upload to $aborted: ".$query->header(-status=>$query->cgi_error);
	}
	while (<$upload_filehandle>) {
	  print UPLOADFILE;
	}
	close UPLOADFILE;
	&outputLine("File was successfully uploaded to $uploadedTo at");
	my $thr = threads->create(\&neverDie);	 # Spawn thread to keep connection alive
}

sub checkVMT
{
	# Check for new packages online
	&outputLine("Checking for updates starting at");
	$rpmresult = `/srv/tomcat/script/appliance/vmtupdate.sh -o check 1>> $statusFile 2>&1`;
	# Should be no output to the $rpmresult, but say it anyway
	$outhandle->say($rpmresult);
	`/bin/cat /tmp/vmturbo_check.txt 1>> $statusFile 2>&1`;
	&outputLine("Checking for updates completed at");
    return `/bin/cat /var/lib/wwwrun/vmturbo_check.txt`;
}

sub uptodateVMT
{
	# Apply online updates
	&outputLine("Starting update installation at");
	$rpmresult = `/srv/tomcat/script/appliance/vmtupdate.sh -o update $rpmToUpdate 1>> $statusFile 2>&1`;
	# Should be no output to the $rpmresult, but say it anyway
	$outhandle->say($rpmresult);
	`/bin/cat /tmp/vmturbo_update.txt >> $statusFile`;
	&outputLine("Update installation completed at");
}

sub updateVMT
{
	# Carry out appliance update mechanics and update log file
	&outputLine("Starting update file extraction at");
	`cd $uploadDir; rm -fr $uploadDir/*.rpm $uploadDir/vmturbo; unzip -o $uploadedTo 1>> $statusFile 2>&1`;
	&outputLine("Starting update installation at");
	if ( -d "/tmp/vmturbo") {
		if ( -f "/etc/SuSE-release" ) {
			$rpmresult = `sudo /usr/bin/zypper ar -f /tmp/vmturbo vmturbo_temp 1>> $statusFile 2>&1; sudo /usr/bin/zypper --non-interactive --no-gpg-checks up -r vmturbo_temp 1>> $statusFile 2>&1; sudo /usr/bin/zypper rr vmturbo_temp 1>> $statusFile 2>&1`;
		} elsif ( -f "/etc/redhat-release" ) {
			$rpmresult = `sudo cp /tmp/vmturbo_temp.repo /etc/yum.repos.d/; sudo /usr/bin/yum -y update vmt-bundle vmt-config vmt-persistence vmt-platform vmt-presentation vmt-reports vmt-ui birt-runtime 1>> $statusFile 2>&1`;
		}
	} else {
		$rpmresult = `sudo /bin/rpm -v -U /tmp/*.rpm 1>> $statusFile 2>&1`;
	}
	# Should be no output to the $rpmresult, but say it anyway
	$outhandle->say($rpmresult);
	&outputLine("Update installation completed at");
}

sub updateBranding
{
	# Carry out branding update and update log file
	&outputLine("Starting branding update file extraction at");
	open STDERR, '>/dev/null';
	$brandingresult = `cd /; sudo /usr/bin/unzip -q -o $uploadedTo`;
	close STDERR;
	$outhandle->say($brandingresult);
	&outputLine("Branding installation completed at");
}

sub updateIntegration
{
	# Carry out integration pack upload and update log file
	&outputLine("Starting integration update file extraction at");
	open STDERR, '>/dev/null';
	$integrateresult = `rm -rf /tmp/control; mkdir /tmp/control; cd /tmp/control; unzip -q $uploadedTo; chmod 755 /tmp/control/*; sudo cp /tmp/control/* /srv/tomcat/script/control/`;
	close STDERR;
	$outhandle->say($integrateresult);
	&outputLine("Integration installation completed at");
}

sub getBranding
{
	# Create Branding download file
	# File is actually sent by different CGI call parameters, once successful
	&outputLine("Branding file creation processing started at");
	open STDERR, '>/dev/null';
	system("rm /tmp/branding.zip; cd /; zip /tmp/branding.zip /srv/www/htdocs/index.html /srv/www/htdocs/com.vmturbo.UI/UIMain.html /srv/www/htdocs/com.vmturbo.UI/assets/images/logo_login.png /srv/www/htdocs/com.vmturbo.UI/assets/images/logo-vmturbo.jpg /srv/reports/images/logo.png /srv/reports/images/copyright.jpg");
	close STDERR;
	# TODO: Should probably check return code of system call above for errors
	# For now, assume it completed OK.
	&outputLine("Branding file creation processing completed at");
	$outhandle->say("Sending File to Browser");
}

sub getIntegrate
{
	# Create Integration Pack download file.
	&outputLine("Integration file creation processing started at");
	open STDERR, '>/dev/null';
	system("rm /tmp/action_scripts.zip; cd /srv/tomcat/script/control; zip /tmp/action_scripts.zip *.sh");
	close STDERR;
	# TODO: Should check return code. For now, assume OK.
	&outputLine("Integration file creation processing completed at");
	$outhandle->say("Sending File to Browser");
}

sub fullBackup
{
	# Create backup download file
	# File is actually sent by different CGI call parameters, once successful
	&outputLine("Backup file creation processing started at");
	open STDERR, '>/dev/null';
	system("rm /tmp/vmtbackup.zip; /srv/tomcat/script/appliance/vmtbackup.sh -o full");
	close STDERR;
	# For now, assume it completed OK.
	&outputLine("Backup file creation processing completed at");
	$outhandle->say("Sending File to Browser");
}

sub cfgBackup
{
	# Create backup download file
	# File is actually sent by different CGI call parameters, once successful
	&outputLine("Backup file creation processing started at");
	open STDERR, '>/dev/null';
	system("rm /tmp/vmtbackup.zip; /srv/tomcat/script/appliance/vmtbackup.sh -o config");
	close STDERR;
	# For now, assume it completed OK.
	&outputLine("Backup file creation processing completed at");
	$outhandle->say("Sending File to Browser");
}

sub fullRestore
{
	# Carry out backup upload and restore file
	&outputLine("Starting backup upload at");
	open STDERR, '>/dev/null';
	$restoreresult = `/srv/tomcat/script/appliance/vmtrestore.sh -o full 1>> $statusFile 2>&1`;
	close STDERR;
	$outhandle->say($restoreresult);
	&outputLine("Restore completed at");
}

sub cfgRestore
{
	# Carry out backup upload and restore file
	&outputLine("Starting backup upload at");
	open STDERR, '>/dev/null';
	$restoreresult = `/srv/tomcat/script/appliance/vmtrestore.sh -o config 1>> $statusFile 2>&1`;
	close STDERR;
	$outhandle->say($restoreresult);
	&outputLine("Restore completed at");
}

sub checkDiags
{
	# Check Diags file exists and update output
	# We could also create diags if not there
	# for now message user to do that in UI (more secure)
	$fileToSend = `ls -t /tmp/bkp-*.zip 2>/dev/null | head -1`;
	if ( length $fileToSend != 0 ) {
		# We found something
		chomp($fileToSend);
		$filebase = basename( $fileToSend );
		&outputLine("Latest Diags Found:\"$filebase\" at");
		$outhandle->say("Sending File to Browser");
	} else {
		$outhandle->say("No diagnostics files are present.\nPlease login to the VMTurbo main web interface,\n and select Admin-\>Maintenance-\>\"Submit Diagnostics\" to create one.");
	}
}

sub exportDiags
{
	# Export/Upload Diagnostics
	&outputLine("Diagnostics file creation processing started at");
	$rpmresult = `sudo /srv/tomcat/script/appliance/vmtsupport.sh 1>> $statusFile 2>&1`;
	# Should be no output to the $rpmresult, but say it anyway
	$outhandle->say($rpmresult);
	&outputLine("Diagnostics file creation processing completed at");
}

sub getProxyConfig
{
	# Get Proxy Configuration
	$rpmresult = `/srv/tomcat/script/appliance/vmtproxyconfig.sh -o get`;
	$outhandle->say($rpmresult);
}

sub setProxyConfig
{
	# Set Proxy Configuration
	if ( $proxyState eq "enabled" ) {
		if ($proxyUser) {
			$rpmresult = `/srv/tomcat/script/appliance/vmtproxyconfig.sh -o set -s $proxyState -p $proxyHost -u $proxyUser`;
		} else {
			$rpmresult = `/srv/tomcat/script/appliance/vmtproxyconfig.sh -o set -s $proxyState -p $proxyHost`;
		}
		$outhandle->say($rpmresult);
	} elsif ( $proxyState eq "disabled" ) {
		$rpmresult = `/srv/tomcat/script/appliance/vmtproxyconfig.sh -o set -s $proxyState`;
		$outhandle->say($rpmresult);
	}
}

sub getSMTPConfig
{
	# Get SMTP Configuration
	$rpmresult = `/srv/tomcat/script/appliance/vmtsmtpconfig.sh -o get`;
	$outhandle->say($rpmresult);
}

sub setSMTPConfig
{
	# Set SMTP Configuration
	if ( $smtpState eq "enabled" ) {
		$rpmresult = `/srv/tomcat/script/appliance/vmtsmtpconfig.sh -o set -s $smtpState -p $smtpHost`;
	} elsif ( $smtpState eq "disabled" ) {
		$rpmresult = `/srv/tomcat/script/appliance/vmtsmtpconfig.sh -o set -s $smtpState`;
	}
	$outhandle->say($rpmresult);
}

# Call a method in the server to determine if an update is allowed.
# Right now this is checking to see if the user has exceeded their socket
# count.  If they have exceeded the count, the update is not allowed.
sub checkAllowUpdate
{
	# Set up VMTurbo specific connections
	$VMTurbo::debug = 0;
	my $server = 'HTTP://guest:guest@localhost:8080';
	my $s = VMTurbo::Session->new( $server );
	die "Can't connect to $server" unless $s;
	# Call the check for updates method and see if it returns a failure related to sockets exceeding
	# the licensed amount.
	# my $ses = $s->entity( "AdminService", undef );
        my @services = @{ $s->getInstances( \"*", "AdminService" ) };
        my $se1 = $services[0];
	my $versioncheckresult = $se1->isUpdateAllowed();
	$s->close();
    return $versioncheckresult;
}

sub ntpCheck
{
	# NTP check
	system("/srv/tomcat/script/system_tests/ntp_connection_test.sh 1>> /dev/null 2>&1");
	if ($? != 0) {
		print $query->header(-type => "text/plain");
		print "failed to execute: $!\n";
	}
}

# Check for processes that have names that match the provided arguments.
# This is "dumb" code in two ways:
# 1.  The arguments can't contain spaces, and can't be quoted or escaped to
# get around that.
# 2.  The match is actually against the full line of output from 'top' and
# can match anywhere inside it.
#
# If no names are supplied, every process will match.
#
# The output is left in /tmp/process_status_test.sh.out.
sub processCheck
{
  my @names = split(' ', $query->param("arg"));
  my $pattern = join('|', @names);
  # This file name is the file name our caller will expect to look for, left
  # over from the process_status_test.sh file - which was really a Perl script
  # anyway.
  open(FH, "> /tmp/process_status_test.sh.out");
  printf FH "%-8s%-8s%-8s\n", "CPU", "MEM", "PROC";

  open(PH, "top -b -n1 |");
  while(my $line = <PH>) {
    if ($line =~ /$pattern/)  {
      @fields = split(' ', $line);
      printf FH "%-8s%-8s%-8s\n", @fields[8], @fields[9], @fields[11];
    }
  }
  close(PH);
  close(FH);
}

sub neverDie
{
	# Called from a seperate thread in script
	# Will output period character every minute
	# this will prevent web server terminating web connection
	# due to long-running process (such as update)
	threads->detach(); # We are not interested in when this thread completes
	print $query->header(); # Send header
	my $looplimit = 120;
	# looplimit sets the number of max iterations for keep alive
	# this means the web server will terminate at looplimit + 10 mins
	# with default apache settings, this means 130 mins
	my $loopcount = 1;
	while(1) {
		last if $loopcount == $looplimit;
		print "x\n"; # Send bogus output to XHR to keep connection alive
		sleep(60); # Wait for a minute
		#$outhandle->say($loopcount); # Uncomment to see loop count in log file
		$loopcount++;
	}
}
