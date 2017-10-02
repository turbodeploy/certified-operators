#!/usr/bin/perl

use strict;

our $version = '$Id: version 0.1$';

my $mySql = '/usr/bin/mysql';
my $sqlString = "'select * from vmtdb.version_info limit 1;'";
my $defaultUser = 'root';
my $defaultPassword = 'vmturbo';

use Getopt::Std;
$Getopt::Std::STANDARD_HELP_VERSION = 1;

our $opt_h;
our $opt_u;
our $opt_p;
our $opt_q;

getopts('hu:p:q');

if ($opt_h) {
    VERSION_MESSAGE();
    HELP_MESSAGE();
}

$opt_u = $defaultUser unless $opt_u;
$opt_p = $defaultPassword unless $opt_p;


if (! system("echo $sqlString |$mySql -u$opt_u -p$opt_p 2>/dev/null 1>/dev/null")) {
    print "access tokens accepted\n" unless $opt_q;
    exit 0;
}

print "access tokens rejected\n" unless $opt_q;
exit 1;



sub VERSION_MESSAGE {
    print("Version: $version");
}

sub HELP_MESSAGE {
    print qq/
    checkSqlPwd [-u user] [-p password] [-q]
        Verifies access to the local mysql database using either the default userid and password
        ($defaultUser\/$defaultPassword), or tokens supplied on the command line with -u and\/or -p.
        Prints a status message indicating whether the access tokens were accepted or rejected.

        As a side effect, exit code is set to 1 if access tokens were not accepted.
            
            -u username		username
            -p password		password
            -q			quiet -- do not print any output, use only exit codes
/;
exit 2
}
