package VMTurbo::Connection;

# Called when the script says: my $c = VMTurbo::connection->new( ... )

# my $s = VMTurbo::Connection->new( "HTTP", "127.0.0.1", 8400 );

sub new {
	my ( $class, $transport, $ip, $port, @tsp_args ) = @_;

	my %self = ( );
	unless (_connect_transport( \%self, $transport, $ip, $port, @tsp_args )) {
		return undef;
	}
	return bless( \%self, $class );

}

sub _connect_transport {
	my ( $self, $transport, $ip, $port, @tsp_args ) = @_;
	if (defined $self->{connection}) {
		VMTurbo::error( "Already connected" );
		return 0;
	}
	my $connection = undef;

	if (uc($transport) eq "HTTP") {
		eval "use VMTurbo::Connection::HTTP;";
		$port = $VMTurbo::default_HTTP_port unless defined $port;
		$connection = VMTurbo::Connection::HTTP->new( $ip, $port, @tsp_args );

	} elsif (uc($transport) eq "HTTPS") {
		eval "use VMTurbo::Connection::HTTPS;";
		$port = $VMTurbo::default_HTTPS_port unless defined $port;
		$connection = VMTurbo::Connection::HTTPS->new( $ip, $port, @tsp_args );

	} elsif (uc($transport) eq "SOCKET") {
		eval "use VMTurbo::Connection::Socket;";
		$port = $VMTurbo::default_Socket_port unless defined $port;
		$connection = VMTurbo::Connection::Socket->new( $ip, $port, @tsp_args );

	} elsif (uc($transport) eq "PSOCKET") {
		eval "use VMTurbo::Connection::Socket;";
		$port = $VMTurbo::default_pSocket_port unless defined $port;
		$connection = VMTurbo::Connection::Socket->new( $ip, $port, proxy=>1, @tsp_args );

	} elsif (uc($transport) eq "SSL") {
		eval "use VMTurbo::Connection::SSL;";
		$port = $VMTurbo::default_SSL_port unless defined $port;
		$connection = VMTurbo::Connection::SSL->new( $ip, $port, @tsp_args );

	} elsif (uc($transport) eq "PSSL") {
		eval "use VMTurbo::Connection::SSL;";
		$port = $VMTurbo::default_pSSL_port unless defined $port;
		$connection = VMTurbo::Connection::SSL->new( $ip, $port, proxy=>1, @tsp_args );

	} else {
		VMTurbo::error( "Unknown transport: '$transport'" );
		return 0;
	}

	if (defined $connection) {
		$self->{transport} = $connection;
		$self->{tsp_name} = $transport;
		$self->{tsp_args} = [ @tsp_args ];
		$self->{ip} = $ip;
		$self->{port} = $port;
		return 1;
	}
	# Connection failed
	return 0;
}

sub _send_request {
	my ( $self, @args ) = @_;
	return $self->{transport}->_send_request( @args );
}

sub _format_request {
	my ( $self, @args ) = @_;
	return $self->{transport}->_format_request( @args );
}

sub _send_requests {
	my ($self, @requests) = @_;
	return $self->{transport}->_send_requests( @requests );
}

sub _read_reply {
	my ( $self, @args ) = @_;
	return $self->{transport}->_read_reply( @args );
}

sub _login {
	my ( $self, @args ) = @_;
	return $self->{transport}->_login( @args );
}

sub _isConnected {
	my ( $self, @args ) = @_;
	return $self->{transport}->_isConnected( @args );
}

sub _close {
	my ( $self, @args ) = @_;
	return $self->{transport}->_close( @args );
}

sub _call {
	my ( $self, @command ) = @_;
	if (!$self->_send_request( @command )) {
		# Transmission failed;
		return undef;
	}
	return $self->_read_reply( );
}

1;
