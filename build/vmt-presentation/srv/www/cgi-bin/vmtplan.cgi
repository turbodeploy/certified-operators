#!/usr/bin/perl -w
#
# The following comment needs to be replaced.
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

$VMTurbo::debug = 0;
my $server = 'HTTP://guest:guest@localhost:8080';

my $s = VMTurbo::Session->new( $server );
die "Can't connect to $server" unless $s;

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

my $loginSession = $s->entity( "LoginService", undef );
my $authenticateresult = $loginSession->authenticate($cgi->param('username'), $cgi->param('userpassword'));
my $userUuid = $loginSession->getUserUUID($cgi->param('username'));
my $viewconfig = $loginSession->findViewConfig($cgi->param('username'));

if ($authenticateresult == 0) {
	print "Content-Type: text/html\n\n";
	print "Authentication failed!\n";
	exit(-1);
}

if ( $viewconfig ne "view-administrator.config.topology") {
	print "Content-Type: text/html\n\n";
	print "User not allowed to plan!\n";
	exit(-1);
}

# Use this to execute a curl command.  It disables proxying for "localhost", which likely
# wouldn't work.  This matters when $apiserver points - as it does by default - to localhost.
my $curl = "/usr/bin/curl --noproxy localhost ";
# The server to which we send requests.
my $apiserver = 'http://administrator:administrator@localhost:8080/vmturbo/api';
my $marketname = &checkQuotedParam("market name", "${userUuid}_BasePlanProjection");

if ($cgi->param('action') eq "Plan P2V") {
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
my $p2vfilename = "/tmp/${filename}";
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
  print "Failed to upload file $!.\n";
}
else {

# Initialize plan
my $result = `$curl -s -X DELETE '${apiserver}/markets/${marketname}'`;
$result = `$curl -s -X POST '${apiserver}/markets/${marketname}' --data 'scope[]='`;

# Instead of copying a plan
#my $result = `$curl -s -X POST '${apiserver}/markets/${marketname}'`;
#my $result = `$curl -s -X DELETE '${apiserver}/markets/${marketname}/entities/GROUP-AllVDCs_${marketname}'`;
#my $result = `$curl -s -X DELETE '${apiserver}/markets/${marketname}/entities/GROUP-PhysicalMachine_${marketname}'`;
#my $result = `$curl -s -X DELETE '${apiserver}/markets/${marketname}/entities/GROUP-Storage_${marketname}'`;
#my $result = `$curl -s -X DELETE '${apiserver}/markets/${marketname}/entities/GROUP-VirtualMachine_${marketname}'`;
#my $result = `$curl -s -X DELETE '${apiserver}/markets/${marketname}/entities/GROUP-Application_${marketname}'`;

# Create templates
my $uuid = `$curl -s -X POST '${apiserver}/templates/VirtualMachine::vm' --data 'numVCPUs=2&vMemSize=2048&vStorageSize=512&networkThroughputConsumed=10&ioThroughputConsumed=10&accessSpeedConsumed=10&memConsumedFactor=.7&cpuConsumedFactor=.5&storageConsumedFactor=.6&vendor=vmturbo&model=opsmgr&desc=myapplication'|grep uuid`;
$uuid =~ /uuid="(.*?)".*/;my $vmuuid = &checkQuotedParam("vmuuid", $1);
$uuid = `$curl -s -X POST '${apiserver}/templates/PhysicalMachine::pm' --data 'numCores=5&cpuCoreSpeed=2000&memSize=2500&networkThroughputSize=511&ioThroughputSize=512&vendor=vendor&model=model&desc=desc&price=1000'|grep uuid`;
$uuid =~ /uuid="(.*?)".*/;my $pmuuid = &checkQuotedParam("pmuuid", $1);
$uuid = `$curl -s -X POST '${apiserver}/templates/Storage::ds' --data 'storageSize=200&accessSpeed=100&price=1000'|grep uuid`;
$uuid =~ /uuid="(.*?)".*/;my $stuuid = &checkQuotedParam("stuuid", $1);

# Import P2V information
open(P2V,$p2vfilename);
my $header1 = <P2V>;
my $header2 = <P2V>;
my $header3 = <P2V>;
while (<P2V>) {
	my @p2ventry = split(/,/);
	my $type = &checkQuotedParam("type", $p2ventry[0]);
	my $count = $p2ventry[1] eq "" ? "1" : &checkQuotedParam("count", $p2ventry[1]);
	my $datacenter = $p2ventry[2] eq "" ? "datacenter" : &checkQuotedParam("datacenter", $p2ventry[2]);
	my $cluster = $p2ventry[3] eq "" ? "cluster" : &checkQuotedParam("cluster", $p2ventry[3]);
	my $network = $p2ventry[4] eq "" ? "network" : &checkQuotedParam("network", $p2ventry[4]);
	my $datastore = $p2ventry[5] eq "" ? "datastore" : &checkQuotedParam("datastore", $p2ventry[5]);
	my $host = $p2ventry[6] eq "" ? "host" : &checkQuotedParam("host", $p2ventry[6]);
	my $name = $p2ventry[7] eq "" ? "name" : &checkQuotedParam("name", $p2ventry[7]);
	my $vendor = $p2ventry[8] eq "" ? "vendor" : &checkQuotedParam("vendor", $p2ventry[8]);
	my $model = $p2ventry[9] eq "" ? "model" : &checkQuotedParam("model", $p2ventry[9]);
	my $desc = $p2ventry[10] eq "" ? "description" : &checkQuotedParam("desc", $p2ventry[10]);
	my $price = $p2ventry[11] eq "" ? "1000" : &checkQuotedParam("price", $p2ventry[11]);

	if ($type eq "VirtualMachine") {
	my $numVCPUs = $p2ventry[12] eq "" ? "2" : &checkQuotedParam("numVCPUs", $p2ventry[12]);
	my $cpuCoreSpeed = $p2ventry[13] eq "" ? "2000" : &checkQuotedParam("cpuCoreSpeed", $p2ventry[13]);
	my $vMem = $p2ventry[14] eq "" ? "2048" : &checkQuotedParam("vMem", $p2ventry[14]);
	my $vStorage = $p2ventry[15] eq "" ? "512" : &checkQuotedParam("vStorage", $p2ventry[15]);

	my $cpuUtil = $p2ventry[26] eq "" ? "0.5" : &checkQuotedParam("cpuUtil", $p2ventry[26]);
	my $memUtil = $p2ventry[28] eq "" ? "0.5" : &checkQuotedParam("memUtil", $p2ventry[28]);
	my $iopsUsed = $p2ventry[32] eq "" ? "10" : &checkQuotedParam("iopsUsed", $p2ventry[32]);
	my $ioUtil = $p2ventry[34] eq "" ? "5" : &checkQuotedParam("ioUtil", $p2ventry[34]);
	my $storageUtil = $p2ventry[35] eq "" ? "5" : &checkQuotedParam("storageUtil", $p2ventry[35]);
	my $networkUtil = $p2ventry[37] eq "" ? "5" : &checkQuotedParam("networkUtil", $p2ventry[37]);

	my $vmtemplateresult = `$curl -s -X POST '${apiserver}/templates/${type}::${name}' --data 'templateUuid=${vmuuid}&numVCPUs=${numVCPUs}&vMemSize=${vMem}&vStorageSize=${vStorage}&networkThroughputConsumed=${networkUtil}&ioThroughputConsumed=${ioUtil}&accessSpeedConsumed=${iopsUsed}&memConsumedFactor=${memUtil}&cpuConsumedFactor=${cpuUtil}&storageConsumedFactor=${storageUtil}&vendor=${vendor}&model=${model}&desc=${desc}'`;
	my $vmresult = `$curl -s -X POST ${apiserver}/markets/${marketname} --data 'templateName=${type}::${name}&count=${count}'`;

# $CLONE_uuid = &checkQuotedParam("CLONE_uuid", $CLONE_UUID);
#		if ($datacenter) {
#			my $datacenterresult = `$curl -X POST '${apiserver}/markets/${marketname}/virtualmachines/${CLONE_uuid}/resources/datacenter/key/${datacenter}'`;
#		}
#		if ($cluster) {
#			my $clusterresult = `$curl -X POST '${apiserver}/markets/${marketname}/virtualmachines/${CLONE_uuid}/resources/cluster/key/${cluster}'`;
#		}
#		if ($network) {
#			my $networkresult = `$curl -X POST '${apiserver}/markets/${marketname}/virtualmachines/${CLONE_uuid}/resources/network/key/${network}'`;
#		}
#		if ($datastore) {
#			my $datastoreresult = `$curl -X POST '${apiserver}/markets/${marketname}/virtualmachines/${CLONE_uuid}/resources/datastore/key/${datastore}'`;
#		}
#		if ($host) {
#			my $hostresult = `$curl -X POST '${apiserver}/markets/${marketname}/virtualmachines/${CLONE_uuid}/resources/host/key/${host}'`;
#		}
	}

	if ($type eq "PhysicalMachine") {
	my $numCores = $p2ventry[12] eq "" ? "16" : &checkQuotedParam("numCores", $p2ventry[12]);
	my $cpuCoreSpeed = $p2ventry[13] eq "" ? "2900" : &checkQuotedParam("cpuCoreSpeed", $p2ventry[13]);
	my $mem = $p2ventry[14] eq "" ? "65536" : &checkQuotedParam("mem", $p2ventry[14]);
#	my $numNetwork = $p2ventry[16] eq "" ? "1" : &checkQuotedParam("numNetwork", $p2ventry[16]);
#	my $networkSpeed = $p2ventry[17] eq "" ? "10000" : &checkQuotedParam("networkSpeed", $p2ventry[17]);
	my $network = $p2ventry[18] eq "" ? "10000" : &checkQuotedParam("network", $p2ventry[18]);
#	my $numIO = $p2ventry[19] eq "" ? "1" : &checkQuotedParam("numIO", $p2ventry[19]);
#	my $ioSpeed = $p2ventry[20] eq "" ? "4000" : &checkQuotedParam("ioSpeed", $p2ventry[20]);
	my $io = $p2ventry[21] eq "" ? "4000" : &checkQuotedParam("io", $p2ventry[21]);

	my $pmtemplateresult = `$curl -s -X POST '${apiserver}/templates/${type}::${name}' --data 'templateUuid=${pmuuid}&numCores=${numCores}&cpuCoreSpeed=${cpuCoreSpeed}&memSize=${mem}&networkThroughputSize=${network}&ioThroughputSize=${io}&vendor=${vendor}&model=${model}&desc=${desc}&price=${price}'`;
	my $pmresult = `$curl -s -X POST '${apiserver}/markets/${marketname}' --data 'templateName=${type}::${name}&count=${count}'`;

#		if ($datacenter) {
#			my $datacenterresult = `$curl -X POST '${apiserver}/markets/${marketname}/hosts/${CLONE_uuid}/services/datacenter/key/${datacenter}'`;
#		}
#		if ($cluster) {
#			my $clusterresult = `$curl -X POST '${apiserver}/markets/${marketname}/hosts/${CLONE_uuid}/services/cluster/key/${cluster}'`;
#		}
#		if ($network) {
#			my $networkresult = `$curl -X POST '${apiserver}/markets/${marketname}/hosts/${CLONE_uuid}/services/network/key/${network}'`;
#		}
#		if ($datastore) {
#			my $datastoreresult = `$curl -X POST ${apiserver}/markets/${marketname}/hosts/${CLONE_uuid}/services/datastore/key/${datastore}`;
#		}
	}

	if ($type eq "Storage") {
	my $storage = $p2ventry[15] eq "" ? "200" : &checkQuotedParam("storage", $p2ventry[15]);
	my $iops = $p2ventry[31] eq "" ? "100" : &checkQuotedParam("iops", $p2ventry[31]);

	my $sttemplateresult = `$curl -s -X POST ${apiserver}/templates/${type}::${name} --data "templateUuid=${stuuid}&storageSize=${storage}&accessSpeed=${iops}&price=${price}"`;
	my $stresult = `$curl -s -X POST ${apiserver}/markets/${marketname} --data "templateName=${type}::${name}&count=${count}"`;

#		if ($host) {
#			my $hostresult = `$curl -X POST ${apiserver}/markets/${marketname}/datastores/${CLONE_uuid}/services/host/key/${host}`;
#		}
	}
}

# Clean up templates
`$curl -s -X POST ${apiserver}/templates/VirtualMachine::vm --data "templateUuid=${vmuuid}&numVCPUs=2&vMemSize=2048&vStorageSize=512&networkThroughputConsumed=10&ioThroughputConsumed=10&accessSpeedConsumed=10&memConsumedFactor=.7&cpuConsumedFactor=.5&storageConsumedFactor=.6&vendor=vmturbo&model=opsmgr&desc=myapplication"`;
my $vmtemplateresult = `$curl -s -X DELETE ${apiserver}/templates/VirtualMachine::vm`;
`$curl -s -X POST ${apiserver}/templates/PhysicalMachine::pm --data "templateUuid=${pmuuid}&numCores=5&cpuCoreSpeed=2000&memSize=2500&networkThroughputSize=511&ioThroughputSize=512&vendor=vendor&model=model&desc=desc&price=1000"`;
my $pmtemplateresult = `$curl -s -X DELETE ${apiserver}/templates/PhysicalMachine::pm`;
`$curl -s -X POST ${apiserver}/templates/Storage::ds --data "templateUuid=${stuuid}&storageSize=200&accessSpeed=100&price=1000"`;
my $sttemplateresult = `$curl -s -X DELETE ${apiserver}/templates/Storage::ds`;

}
}

if ($cgi->param('action') eq "Plan V2V") {
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
if ($filename =~ /^\.\./) {
  die "Filename started with .. !$fu!$filename!";
}
my $p2vfilename = "/tmp/${filename}";
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
  print "Failed to upload file $!.\n";
}
else {
my $result = `$curl -s -X DELETE '${apiserver}/markets/${marketname}'`;
my $marketSession = $s->entity( "MarketConfigService", undef );
my $loadresult = $marketSession->loadTopology($p2vfilename);
}
}

# Run the plan
my $result = `$curl -s -X POST '${apiserver}/markets/${marketname}' --data 'state=run'`;

print "Content-Type: text/html\n\n";
my $actionurl="/vmturbo/api/actionlogs/${marketname}_ActionLog/actionitems";
my $reporturl1="/persistence/standard_reports/get_planner_report/1.pdf";
my $reporturl2="plan_name=${marketname}&plan_display_name=P2V_Plan&customer_name=VMTurbo";
my $reporturl=join("?",$reporturl1, $reporturl2);
print "<a href=${actionurl}>View Action Plan</a><p>";
print "<a href=${reporturl}>Generate Report</a>";

$s->close();
