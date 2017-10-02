package VMTurbo::Connection::SSL;

use Net::SSL;
use IO::Socket;
use URI::Escape;
use VMTurbo::Connection::Socket;

our @ISA = ( "VMTurbo::Connection::Socket" );

our $debug = 0;

sub new {
	my ( $class, $ip, $port, %options) = @_;

	my $proxy_to = $options{proxy_to};
	if ($options{proxy}) { $proxy_to = $VMTurbo::default_pSSL_proxy_to; }

	my $sock = undef;

	if (defined $proxy_to) {
		my ( $proxy_to_addr, $proxy_to_port ) = split(/:/, $proxy_to, 2);
		local $ENV{HTTPS_PROXY} = "$ip:$port";
		$sock = new Net::SSL(
			PeerAddr => $proxy_to_addr,
			PeerPort => $proxy_to_port,
			Proto => "tcp",
			# SSL_Version => 3,
		);
	} else {
		$sock = new Net::SSL(
			PeerAddr => $ip,
			PeerPort => $port,
			Proto => "tcp",
			# SSL_Version => 3,
		);
	}

	unless (defined $sock) {
		VMTurbo::error( "Failed to connect to Socket $ip:$port" );
		return undef;
	}

	my %this = (
		ip => $ip,
		port => $port,
		connection => $sock,
		transport => "SSL",
		debug => $debug
	);

	my $banner = _readline( \%this );

	unless (defined($banner) && $banner =~ m/^\+VMTurbo/) {
		VMTurbo::error("Cant read welcome banner");
		close($sock);
		return undef;
	}

	$this{peercert} = $sock->get_peer_certificate();

	return bless \%this, $class;
}

sub _readline {
	my ( $this ) = @_;
	while ($this->{buffer} !~ m/\n/s) {
		my $got = $this->{connection}->getchunk();
		if (!defined($got) || $got eq "") {
			VMTurbo::error( "Read error" );
			return undef;
		}
		$this->{buffer} .= $got;
	}
	my ($rtn, $buff) = split(/\n/, $this->{buffer}, 2);
	$this->{buffer} = $buff;
	return $rtn."\n";
}

1;
