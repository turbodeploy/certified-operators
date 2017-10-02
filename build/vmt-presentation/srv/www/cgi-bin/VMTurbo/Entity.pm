package VMTurbo::Entity;

# usage:
# 	$reference = VMTurbo::Entity->_new( $session, $class, $uuid );
# or
# 	$reference = VMTurbo::Entity->_new( $session, $class );
# where:
# 	$session
# 		is the handle of the session that maps the connection to the
# 		VMT server.
# 	$class
# 		is the name of the EMF class
# 	$uuid
# 		is the UUID of the entity. If this is omitted or is undef, then a
# 		reference to a singleton class entity is returned.
# note:
# 	Use the "entity" function of VMTurbo::Session instead of this function

sub _new {
	my ( $tieclass, $session, $class, $uuid ) = @_;
	my %self = ();
	tie %self, "VMTurbo\::Entity\::$class", {
		-session => $session,
		-class => $class,
		defined($uuid) ? (-uuid => $uuid) : ( ),
		-singleton => !defined($uuid)
	};
	return bless ( \%self, "VMTurbo\::Entity\::$class" );
}

sub _invoke {
	my $self = shift @_;
	my $arg1 = shift @_;
	my ( $option, $operation );

	if (ref($arg1) eq "SCALAR") {
		$option = ${$arg1};
		$operation = shift @_;
	} else {
		$option = "";
		$operation = $arg1;
	}

	my $info = $self->{-session}->{opmap}->{$self->{-class}}->{$operation}->{0+@_};
	unless (defined $info) {
		my $n = 0 + @_;
		VMTurbo::error( "Cant find operation signature '$operation#$n' for class '$self->{-class}'\n" );
		return undef;
	}

	unless (@{$info} == 1) {
		my $n = 0 + @_;
		VMTurbo::error( "Wrong number of signatures for operation '$operation#$n' in class '$self->{-class}'\n" );
		return undef;
	}
	my ( $opid, $rtntype, @argtypes) = @{$info->[0]};
	my @args = @_;
	for (my $i = 0; $i < @args; $i++) {
		if ($argtypes[$i] eq "b") {
			$args[$i] = $args[$i] ? "true" : "false";
		}
	}
	if ($self->{-singleton}) {
		return $self->{-session}->invoke( \"c$option", $self->{-class}, $opid, @args );
	} else {
		return $self->{-session}->invoke( \$option, $self->{-uuid}, $opid, @args );
	}
}

sub _get {
	my $self = shift @_;
	my $option = "";
	if (@_ > 0 && ref($_[0]) eq "SCALAR") {
		$option = ${shift @_};
	}

	return $self->{-session}->get(\$option, $self, @_);
}

sub _put {
	my $self = shift @_;
	my $option = "";
	if (@_ > 0 && ref($_[0]) eq "SCALAR") {
		$option = ${shift @_};
	}

	return $self->{-session}->put(\$option, $self, @_);
}

sub _invoke_op_name {
	my ( $operation, $self, @args ) = @_;

	my $option = "";
	if (@args > 0 && ref($args[0]) eq "SCALAR") {
		$option = ${shift @args};
	}

	my $info = $self->{-session}->{opmap}->{$self->{-class}}->{$operation}->{0+@args};
	unless (defined $info) {
		VMTurbo::error("Operation signature not found [maybe: wrong number of arguments]");
		return undef;
	}
	my $num_signatures = @{$info};
	if ($num_signatures > 1) {
		VMTurbo::error("Ambiguous operation signature");
		return undef;
	}

	return _invoke( $self, \$option, $operation, @args );
}

sub _invoke_op_id {
	my ( $id, $nargs, $self, @args ) = @_;

	my $option = "";
	if (@args > 0 && ref($args[0]) eq "SCALAR") {
		$option = ${shift @args};
	}

	if (@args != $nargs) {
		VMTurbo::error("Wrong number of arguments");
		return undef;
	}

	return _invoke( $self, \$option, $id, @args );
}

sub _getUuid {
	my ( $this ) = @_;

	if ($this-{-singleton}) {
		return $this->{uuid};
	} else {
		return $this->{-uuid};
	}
}

sub _cache_keys {
	my ( $this ) = @_;

	my $class = $this->{-class};
	return if exists $this->{-session}->{keycache}->{$class};
	my @props = sort { lc($a) cmp lc($b) } @{$this->{-session}->getProperties( \"a", $class )};
	my %order = ( '' => $props[0], $props[-1] => undef );
	for (my $n = 0; $n < @props-1; $n++) {
		$order{$props[$n]} = $props[$n+1];
	}
	$this->{-session}->{keycache}->{$class} = \%order;
}

sub TIEHASH {
	my ( $pkg, $hash ) = @_;
	return (bless $hash, $pkg);
}

sub STORE {
	my ( $this, $key, $value ) = @_;
	if ($key eq ".") { return $value; }
	if ($key =~ m/^-/) { $this->{$key} = $value; return $value; }
	my ($k, $opt) = split(/\./, $key, 2);
	$this->_put( \$opt, $k, $value );
	return $value;
}

sub FETCH {
	# We are called from a data dumper type of function (one that walks hashes, and could
	# potentially enter a walk loop) we force object references to return as UUIDs. The list
	# of packages for which this is an issue is user-modifiable.
	my @c = caller();
	my $opt = "";
	foreach my $walker (@VMTurbo::walkers) {
		if ( $c[0] eq $walker ) { $opt = "u"; }
	}

	my ( $this, $key ) = @_;
	# The key can be..
	# 1: A reference to an array, containing attribute names.
	# 2: A string with space-or-comma delimited names listed
	# 3: A string with just one name name.
	if (ref($key) eq "ARRAY") {
		return $this->_get( \$opt, @{$key} );
	} else {
		my @keys = split(/[ ,]+/, $key);
		if (@keys == 1) {
			if ( $key eq ".") { return $this; }
			if ( $key =~ m/^-/ ) { return $this->{$key}; }
			return $this->_get( \$opt, $key );
		} elsif (@keys > 1) {
			return $this->_get( \$opt, @keys );
		} else {
			VMTurbo::error( "No keys specified" );
			return undef;
		}
	}
}

sub FIRSTKEY {
	my ( $this ) = @_;
	_cache_keys( $this );
	my $class = $this->{-class};
	return $this->{-session}->{keycache}->{$class}->{''};
}

sub NEXTKEY {
	my ( $this, $lastkey ) = @_;
	_cache_keys( $this );
	my $class = $this->{-class};
	return $this->{-session}->{keycache}->{$class}->{$lastkey};
}

sub EXISTS {
	my ( $this, $key ) = @_;
	return 1 if $key eq ".";
	_cache_keys( $this );
	my $class = $this->{-class};
	return defined $this->{-session}->{keycache}->{$class}->{$key};
}

sub DELETE {
	my ( $this, $key ) = @_;
}

sub CLEAR {
	my ( $this ) = @_;
}

sub SCALAR {
	my ( $this ) = @_;
}

1;
