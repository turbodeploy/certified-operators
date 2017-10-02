#!/usr/bin/perl -w
# Script to handle VMTurbo reports from Flash
# Alec Kemp VMTurbo 2012
# Script is called 2 ways, defined by CGI param calltype
# ACTION - do something
# DOWN - send back the contents of a file that has been requested. This happens after ACTION has
# completed (which creates the file).
# Each 'way' has multiple types of behavior, controlled by actiontype CGI param e.g. generate
# report file etc

#use strict;
#use Time::gmtime;
use CGI qw ( -unique_headers );
# Need to use IO as standard perl file handling seems to lock exclusive
use IO::File qw();
# Suppress redundant HTTP Headers
$CGI::HEADERS_ONCE = 1;
use vars qw($reportresult);
# TODO: Probably dont want carp fatals in browser in GA
#use CGI::Carp qw ( fatalsToBrowser );
use CGI::Carp qw ();
use File::Basename;
use VMTurbo;

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

# Set max file size limit here - currently 70MB. File will be rejected post-upload if
# over this size.
# The JS should also enforce this pre-upload, but as client code it cannot be trusted.
# param() returns empty set if this is triggered
$CGI::POST_MAX = 1024 * 70000;

# Set up CGI for form data
my $query = new CGI;

my $upload_dir = "/tmp/";
# Set up variables from calling form
my $vmtuser = $query->param("userName");
my $vmtuuid = $query->param("userUUID");
my $vmtpassword = $query->param("password");
# calltype is ACTION for doing, READ for the output text, and DOWN for file stream download
my $calltype = $query->param("callType");
# What the CGI should do
my $actiontype = $query->param("actionType");
# actiontype is compared to specific constants, but is also used in
# constructing the name of the status file, which may be opened before we
# actually match an action type.
die "Not a valid action type" unless ($actiontype =~ /^[A-Z]+$/);

# Mandatory report generation attributes
my $format = &getQSafeParam("format");
my $output = &getQSafeParam("output");
my $rptdesign = &getQSafeParam("rptdesign");

# Optional report generation attributes
my $parameters = "";
if (defined $query->param("uuid")) {
	$parameters .= " -p selected_item_uuid='".&getQSafeParam("uuid")."'";
}
if (defined $query->param("instance_name")) {
	$parameters .= " -p pm_name='".&getQSafeParam("instance_name")."'";
	$parameters .= " -p selected_item_name='".&getQSafeParam("instance_name")."'";
	$parameters .= " -p plan_name='".&getQSafeParam("instance_name")."'";
}
if (defined $query->param("vm_group_name")) {
	$parameters .= " -p vm_group_name='".&getQSafeParam("vm_group_name")."'";
}
if (defined $query->param("customer_name")) {
	$parameters .= " -p customer_name='".&getQSafeParam("customer_name")."'";
}
if (defined $query->param("num_days_ago")) {
	$parameters .= " -p num_days_ago='".&getQSafeParam("num_days_ago")."'";
}
if (defined $query->param("hide_charts")) {
	$parameters .= " -p hide_charts='".&getQSafeParam("hide_charts")."'";
}
if (defined $query->param("shared_customer")) {
 $parameters .= " -p shared_customer='".&getQSafeParam("shared_customer")."'";
}

# Output filename
my $outfile = "";
# filename to send back progress from
my $statusfile = "$actiontype.vmturbo.txt";

my @months = qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
my @weekDays = qw(Sun Mon Tue Wed Thu Fri Sat Sun);

if ($calltype eq "ACTION" ) {
	# Carry out action
	# Every run has a VERSIONS and AUTHENTICATE
	# plus another actiontype
	if ( $actiontype eq "VERSIONS" ) {
		# Initial call, to populate version information popup
		&versionInfo;
		exit();
	}

	if ( $actiontype eq "AUTHENTICATE" ) {
		# Initial user check, and also called with each action,
		# in case user modifies username password after initial
		# authentication. Client-side JS should prevent this
		# but cannot be trusted.
		&authenticate;
		print $query->header();
		print "SUCCESS";
		exit();
	}

	# create output file with expected name in upload dir
	$outfile = $upload_dir.$statusfile;
	# create file for output progress
	$outhandle = IO::File->new($outfile, 'a');
	die "Could not append to $outfile: $!" unless $outhandle;
	$outhandle->autoflush(1);
	
	if ( $actiontype eq "GENERATE" ) {
		&authenticate;
		# If we get this far, authentication is good
		&genReport;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
		exit();
	}
	elsif ( $actiontype eq "MAKE" ) {
		&authenticate;
		&makePdf;
		$outhandle->close();
		# Prevent HTTP 500
		print $query->header();
		exit();
	}
}
elsif ( $calltype eq "DOWN" ) {
	# Send back contents of file we created
	if ( $actiontype eq "REPORT" ) {
		if ( $output =~ /^\/srv\/reports\/pdf_files\//
		     and !($output =~ /\.\./)) {
			open(DLFILE, $output);
			@fileholder = <DLFILE>;
			close (DLFILE);
			$filebase = basename( $output );
			if ( $format eq "pdf" ){
				print "Content-Type:application/pdf\n";
				print "Content-Disposition:inline;filename=".$filebase."\n\n";
			}elsif ( ( $format eq "xlsx" ) || ( $format eq "xls" )|| ( $format eq "csv" ) )	{
				print "Content-Type:application/excel\n";
				print "Content-Disposition:attachment;filename=".$filebase."\n\n";
			}
			print @fileholder;
			exit();
		} else {
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
# uncomment for debug
#print $data;
exit(-1);

# Check a value for safety for use as in a shell command line ***between
# single quotes***.  Call as &checkQuotedParam($name, $value).  The first
# parameter is a name used only in error messages.  Returns $value if it's
# safe; otherwise, calls die.
sub checkQuotedParam
{
  my $name = shift;
  my $param = shift;
  unless ($param =~ m'^[] [a-zA-Z0-9|`~!@#$%^&*()_=+}{};:"<>,./\\?-]+$') {
    die "Parameter $name contains invalid characters: '$param'";
  }
  return $param;
}

# Retrieve the value of a parameter for use as in a shell command line
# ***between single quotes***.  Returns the parameter's value if it's safe;
# otherwise, calls die.
sub getQSafeParam
{
  my $paramName = shift;
  my $param = $query->param($paramName);
  return (defined $param) ? &checkQuotedParam($paramName, $param) : $param;
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

sub authenticate
{
	# Set up VMTurbo specific connections
	$VMTurbo::debug = 0;
	my $server = 'HTTP://guest:guest@localhost:8080';
	my $s = VMTurbo::Session->new( $server );
	die "Cant connect to $server" unless $s;
	# Check the VMTurbo user specifed exists, and is allowed to update (admin user)
	my $ses = $s->entity( "LoginService", undef );
	my $authenticateresult = -1;
	if ($vmtpassword) {
		$authenticateresult = $ses->authenticate($vmtuser, $vmtpassword);
	}
	elsif ($vmtuuid) {
		$authenticateresult = $ses->authenticateByCookie($vmtuser, $vmtuuid);
	}
	$s->close();
	if ($authenticateresult == 0) {
	 print $query->header();
		 print "FAIL";
		 exit(-1);
	}
}

sub genReport
{
	# Check for new packages online
	my $directOutput = $upload_dir.$statusfile;
	&outputLine("Report generation starting at");
	my $outputEnc = CGI::escape($output);
	$gen_report = '. /etc/sysconfig/persistence;'
			.'$BIRT_HOME/ReportEngine/genReport.sh '
			."--format '$format' --output '$output' ".$parameters." '$rptdesign';"
			." /usr/bin/curl -u guest:guest"
                        ." --noproxy localhost http://localhost:8080/vmturbo/api\\?inv.u"
			.           "\\&ReportingManager\\&reportReady\\&'$outputEnc'";
	$outhandle->say($gen_report." ".gmtime()." UTC");
	system("($gen_report) 1>> $directOutput 2>&1 &");
}

sub makePdf
{
	my $directOutput = $upload_dir.$statusfile; #TODO: redirect the logging elsewhere
	&outputLine("Report generation starting at");

	my $rptdesignEnc = CGI::escape($rptdesign);
	$make_pdf = 'echo `date +\"%Y/%m/%d-%H:%M\"`;'
			.". /etc/sysconfig/persistence;"
			." /srv/reports/bin/make_pdf.sh '$rptdesign';"
			." /usr/bin/curl -u guest:guest"
                        ." --noproxy localhost http://localhost:8080/vmturbo/api\\?inv.u"
			.           "\\&ReportingManager\\&reportReady\\&'$rptdesignEnc'";
	$outhandle->say($make_pdf." ".gmtime()." UTC");
	system("($make_pdf) 1>> $directOutput 2>&1 &");
}
#TODO: add method with action MAKE and run this command:
#puts "echo `date +\"%Y/%m/%d-%H:%M\"` -- #{report.filename.to_s}"
#puts "$VMT_REPORTS_HOME/bin/make_pdf.sh #{report.filename.to_s}"
#Be careful about quoting!
