package VMTurbo::Connection::HTTP;

use strict;
use URI::Escape;
use MIME::Base64;

our $debug = 0;

# syntax: $s = VMTurbo::Connection::HTTP->new( $ip_address, $port );
sub new {
	my ( $class, $ip, $port ) = @_;

	my $s = _connect( $class, $ip, $port );
	unless (defined $s) {
		VMTurbo::error( "Failed to connect to $ip:$port" );
		return undef;
	}
	my @class = split(/::/, $class);
	my %this = (
		ip => $ip,
		port => $port,
		class => $class,
		connection => $s,
		transport => $class[-1],
		txnnum => 0,
		debug => $debug
	);
	$this{peercert} = undef;
	if ( $this{transport} eq "HTTPS" ) {
		$this{peercert} = $this{connection}->get_peer_certificate();
	}
	return bless \%this, $class;
}

sub _connect {
	my ( $class, $ip, $port ) = @_;

	my $ipport = "$ip:$port";

	if ($class =~ m/HTTP$/) {
		eval "use Net::HTTP;";
		return Net::HTTP->new( Host => $ipport, KeepAlive => 1 );

	} elsif ($class =~ m/HTTPS$/) {
		eval "use Net::HTTPS;";
		return Net::HTTPS->new( Host => $ipport, KeepAlive => 1 );
	}

	return undef;
}

sub _close {
	my ( $this, @args ) = @_;
	my $rtn = $this->{connection}->close();
	$this->{connection} = undef;
	return $rtn;
}

sub _isConnected {
	my ( $this ) = @_;
	return (defined $this->{connection});
}

# Returns true(1) or false(0)
sub _send_request {
	my ( $this, @cmd ) = @_;
	if (!_isConnected($this)) {
		VMTurbo::error( "Connection not open" );
		return 0;
	}
	my @encoded = @cmd;
	foreach (@encoded) { $_ = uri_escape($_); }

	$this->{txnnum} ++;
	if ($this->{txnnum} > 80) {
		$this->{connection}->close();
		$this->{connection} = _connect($this->{class}, $this->{ip}, $this->{port});
		unless (defined $this->{connection}) {
			VMTurbo::error("Connection to $this->{ip}:$this->{port} list");
			return undef;
		}
		$this->{txnnum} = 0;
	}

	my @auth = ();
	if (defined($this->{user}) || defined($this->{password})) {
		@auth = (
			"Authorization",
			"Basic " . encode_base64($this->{user} . ":" . $this->{password})
		);
	}
	if ($this->{debug}) {
		print "\n>> GET /vmturbo/api?".join("&", @encoded)."\n";
	}
	my $rtn = $this->{connection}->write_request(
		GET => "/vmturbo/api?".join("&", @encoded ),
		@auth
	) ? 1 : 0;

	return $rtn;
}

# Returns a hash. Keys are..
#	state	 OK or ERR
# 	type	 The data type contained
# 	encoding The encoding method used
# 	mesg	 The data content (potentially encoded)
sub _read_reply {
	my ( $this ) = @_;
	if (!_isConnected($this)) {
		VMTurbo::error( "Connection not open" );
		return 0;
	}
	my ( $code, $mess, @h ) = $this->{connection}->read_response_headers( );
	my %h = @h;
	if ($this->{debug}) {
		print "<< $code $mess\n";
		while (@h) {
			print "<< ".(shift @h).": ".shift(@h)."\n";
		}
	}
	unless (defined $code) {
		return { state=>"ERR", type=>"s", encoding=>"raw", mesg=>"Failure reading HTTP response headers" };
	}
	if ( $code ne "200" ) {
		my $buff = "";
		while (1) {
			my $n = $this->{connection}->read_entity_body( $buff, 8 * 1024 );
			last unless $n;
		}
		if ($code eq "401") {
			return { state=>"ERR", type=>"s", encoding=>"raw", mesg=>"Authorization failed" };
		}
		return { state=>"ERR", type=>"s", encoding=>"raw", mesg=>"HTTP status $code" };
	}

	if ( $this->{transport} eq "HTTPS" && !defined($this->{peercert}) ) {
		$this->{peercert} = $this->{connection}->get_peer_certificate();
	}

	my $rtn = "";
	while (1) {
		my $buf = "";
		my $n = $this->{connection}->read_entity_body( $buf, 8 * 1024 );
		unless (defined $n) {
			return { state=>"ERR", type=>"s", encoding=>"raw", mesg=>"HTTP read failure" };
		}
		last unless $n;
		$rtn .= $buf;
	}
	if ($this->{debug}) {
		print "<<\n";
		foreach my $line (split(/\r?\n/, $rtn)) {
			print "<< $line\n";
		}
	}
	my $state = $h{"X-VMTurbo-State"};
	my $info = $h{"X-VMTurbo-Info"};
	my ( $type, $encoding, $cont_len, $pad_len ) = split( " ", $info );
	return { state=>$state, type=>$type, encoding=>$encoding, mesg => substr($rtn, 0, $cont_len) };
}

sub _login {
	my ( $this, $option, $user, $password ) = @_;
	if (!_isConnected($this)) {
		VMTurbo::error( "Connection not open" );
		return 0;
	}
	$this->{user} = $user;
	$this->{password} = $password;

	# Now check that the remembered username and password are correct by trying
	# an "null-operation" exchange.

	if ($option =~ m/t/) {
		if (!$this->_send_request( "noop" )) {
			return 0;
		}
		my $reply = $this->_read_reply();
		unless (defined $reply) {
			VMTurbo::error("Failed to send login check request");
			return 0;
		}
		unless ($reply->{state} eq "OK") {
			VMTurbo::error( $reply->{mesg} );
			return 0;
		}
	}
	return 1;
}

1;
