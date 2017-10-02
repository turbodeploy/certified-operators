package VMTurbo::Connection::Socket;

use IO::Socket;
use URI::Escape;
use MIME::Base64;

our $debug = 0;

sub new {
	my ( $class, $ip, $port, %options) = @_;

	my $sock = new IO::Socket::INET(
		PeerAddr => $ip,
		PeerPort => $port,
		Proto => "tcp"
	);

	unless (defined $sock) {
		VMTurbo::error( "Failed to connect to Socket $ip:$port" );
		return undef;
	}

	my %this = (
		ip => $ip,
		port => $port,
		connection => $sock,
		transport => "Socket",
		debug => $debug
	);

	my $proxy_to = $options{proxy_to};
	if ($options{proxy}) { $proxy_to = $VMTurbo::default_pSocket_proxy_to; }

	if (defined $proxy_to) {
		print $sock "CONNECT $proxy_to HTTP/1.0\r\n\r\n";
		my $connect_reply = <$sock>;
		unless ($connect_reply =~ m!^HTTP/\d+.\d+ 200 !) {
			$connect_reply =~ s/\r?\n$//s;
			VMTurbo::error("Unexpected proxy connect response: $connect_reply");
			close($sock);
			return undef;
		}
		while (1) {
			my $line = <$sock>;
			unless (defined $line) {
				VMTurbo::error("Unexpected proxy connect response");
				close($sock);
				return undef;
			}
			$line =~ s/\r?\n$//s;
			last if ($line eq "");
		}
		$this{proxy_to} = $proxy_to;
	}

	my $banner = <$sock>;
	unless (defined($banner) && $banner =~ m/^\+VMTurbo/) {
		VMTurbo::error("Cant read welcome banner");
		close($sock);
		return undef;
	}
	return bless \%this, $class;
}

sub _readline {
	my ( $this ) = @_;
	my $conn = $this->{connection};
	return <$conn>;
}

sub _isopen {
	my ( $this ) = @_;
	return (defined $this->{connection});
}

sub _isConnected {
	my ( $this ) = @_;
	return 0 unless _isopen($this);
	return $this->{connection}->connected() ? 1 : 0;
}

sub _close {
	my ( $this, @args ) = @_;

	my $rtn = $this->{connection}->close();
	$this->{connection} = undef;
	return $rtn;
}

sub _send_request {
	my ( $this, @cmd ) = @_;
	if (!_isopen($this)) {
		VMTurbo::error( "Connection not open" );
		return 0;
	}
	my $request = $this->_format_request( @cmd );
	if ($this->{debug}) { print ">> $request"; }
	my $conn = $this->{connection};
	$conn->write( $request );
	return 1;
}

sub _format_request {
	my ( $this, @cmd ) = @_;
	my @encoded = @cmd;
	foreach (@encoded) { $_ = uri_escape($_); }
	return join(" ",@encoded)."\n";
}

sub _send_requests {
	my ( $this, @requests ) = @_;
	if (!_isopen($this)) {
		VMTurbo::error( "Connection not open" );
		return 0;
	}
	if ($this->{debug}) {
		foreach my $req (@requests) {
			print ">> $req\n";
		}
	}
	my $conn = $this->{connection};
	$conn->write(join("\n", @requests, ""));
	return 1;
}

sub _read_reply {
	my ( $this ) = @_;

	if (!_isopen($this)) {
		VMTurbo::error( "Connection not open" );
		return undef;
	}

	my $request = $this->_format_request( @cmd );
	my $header = $this->_readline();
	if ($this->{debug}) { print "<< ".$header; }
	if ($header =~ m/^(\+|-)(.*?) (.*?) (\d+) (\d+)\r?\n$/) {
		my $oktag = $1;
		my $type = $2;
		my $encoding = $3;
		my $c_len = $4;
		my $p_len = $5;
		my $content = "";
		while (length($content) < ($c_len + $p_len)) {
			$content .= $this->_readline();
		}
		if ($this->{debug}) {
			my @content = split( /\r?\n/, $content );
			foreach (@content) { print "<< $_\n"; }
		}
		return { state => ($oktag eq "+" ? "OK" : "ERR"), type => $type, encoding => $encoding, mesg => substr($content, 0, $c_len) };
	} else {
		VMTurbo::error( "Bad returned header: $header" );
		return undef;
	}
}

sub _login {
	my ($this, $option, $user, $password) = @_;

	if (!_isopen($this)) {
		VMTurbo::error( "Connection not open" );
		return 0;
	}

	my $enc = encode_base64($user . ":" . $password);
	chomp $enc;
	if (!$this->_send_request( "user", $enc )) {
		return 0;
	}
	my $reply = $this->_read_reply();
	unless (defined $reply) {
		VMTurbo::error("Failed to send login request");
		return 0;
	}
	unless ($reply->{state} eq "OK") {
		VMTurbo::error( $reply->{mesg} );
		return 0;
	}

	$this->{user} = $user;
	$this->{password} = $password;

	return 1;
}

1;
