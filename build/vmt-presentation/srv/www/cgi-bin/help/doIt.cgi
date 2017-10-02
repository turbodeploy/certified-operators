#!/usr/bin/perl -w
#

use strict;
use CGI;
#use CGI::Carp 'fatalsToBrowser';

my $query = new CGI;

my $basePath = "/srv/www/htdocs/com.vmturbo.UI/help";
my $sep = "/";

my $func = $query->param('FUNC');

if($func eq 'load_xml_file') {
	print $query->header( "text/html; charset=UTF-8" );
	my $path = $query->param('xml_path');
	my $location = $query->param('location');
	my $type = $query->param('type');
	#Security
	if(isUnsafeStr($path)) { exit(1); }
	if(isUnsafeStr($type)) { exit(1); }
	if(isInvalidLocation($location)) { exit(1); }

	my $fullPath = $basePath.$sep.$location.$sep.$path.".".$type;
	if (open(XMLFILE, $fullPath)) {
		while (<XMLFILE>){
		   print;
		}
		close(XMLFILE);
	}
}

sub isUnsafeStr {
	if($_[0] =~ /^[A-Za-z0-9_]+$/) {return(0);}
	return(1);
}
sub isInvalidLocation {
	my $param = shift;
    if("_Topics" eq $param) { return(0); }
    if("_Target_Configuration_Topics" eq $param) { return(0); }
	if("_API_Topics" eq $param) { return(0); }
	if("maps" eq $param) { return(0); }
	if("xsl" eq $param) { return(0); }
	if("data" eq $param) { return(0); }
	return(1);
}