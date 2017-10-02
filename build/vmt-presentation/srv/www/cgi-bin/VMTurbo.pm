package VMTurbo;

use strict;
use VMTurbo::Session;
use VMTurbo::Connection;
use VMTurbo::Entity;

our $debug = 0;
our $last_error = undef;

our $default_HTTP_port = 80;
our $default_HTTPS_port = 443;
our $default_Socket_port = 8401;
our $default_pSocket_port = 80;
our $default_SSL_port = 8402;
our $default_pSSL_port = 80;

our $default_pSocket_proxy_to = "127.0.0.1:$default_Socket_port";
our $default_pSSL_proxy_to = "127.0.0.1:$default_SSL_port";

# Packages that walk hashes with the potential for entering never-ending loops when
# walking the VMT entities. When packages listed here call our entity tied hash logic
# the "u" option is used - to cause referenced entitites to be returned as UUIDs.
# See "FETCH" in VMTurbo/Entity.pm for the place where this logic is impelemented.
our @walkers = ( "Data::Dumper", "dumpvar" );

# override this if you want alternative error handling
sub error {
	my ( $msg ) = @_;
	$last_error = $msg;
	if ($debug) { print STDERR "ERROR: $msg\n"; }
}

# override this if you want alternative XML parsing logic.
our $xs = undef;
sub parseXML { 
	my ( $str ) = @_;
	if ($str =~ m/^\s*$/) { return { }; }
	unless (defined $xs) {
		eval "
			use XML::Simple;
			\$xs = XML::Simple->new();
		";
		if ($@) { die $@; }
	}
	return $xs->parse_string($str);
}

#===========================================================================================#
# Elements BELOW this point should NOT be over-ridden in your script.                       #
#===========================================================================================#

our @use_classes = ( );
our $n_bits = 0;

sub import {
	my $module = shift @_;
	@use_classes = @_;
	$n_bits = length(sprintf('%x', -1)) * 4;
}

1;

