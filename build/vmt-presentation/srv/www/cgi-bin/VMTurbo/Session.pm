package VMTurbo::Session;

use VMTurbo;
use VMTurbo::Connection;
use VMTurbo::Entity;
use URI::Escape;
use Data::Dumper;

if ($VMTurbo::n_bits < 64) {
	eval "use Math::BigFloat;";
}

our $func = "";

=head1 NAME

VMTurbo::Session - Connect to a VMTurbo server, and work with its data structures.

=head1 A FIRST EXAMPLE

A simple VMTurbo Perl API script will normally look something like this..

 # Load the VMTurbo packages.
 use VMTurbo;

 # The transport, user, password and address of the server to use.
 my $server_url = "pSocket://guest:drowsap\@vmt-appliance:80";

 # Indicate that the API should display error messages for you.
 $VMTurbo::debug = 1;

 # Attempt to establish the connection.
 my $s = VMTurbo::session->new( $server_url );
 die "Failed to connect" unless $s;

 # Do some work with the server.

 # For example: List all the VMs (class and displayname)
 my @vms = @{ $s->getInstances( \"v", "VirtualMachine" ) };
 foreach my $record (@vm) {
     print "$vm[0] : $vm[3]\n";
 }

 # Or maybe .. do the same with a specific PM..
 my $pm_name = "esx10.vmturbo.com";
 my $pm_ref = $s->entity( "PhysicalMachine", $pm_name );
 print "$pm_ref->{creationClassName} : $pm_ref->{displayName}\n";

 # Or maybe .. run an operation against an entity
 my$ttl = $pm_ref->timeToLive();
 print "TTL = $ttl\n";

 # Close the connection and terminate
 $s->close();
 exit 0;

=head1 USE

The perl "use" function is used to load the API library into the script's
name space, and optionally pre-load some classes.

=over 4

=item Syntax

 use VMTurbo [@class_list];

=item Where

=over

=item @class_list

An optional list of classes to be loaded into the perl name space. Note that preloading
is not normally required since classes are loaded when the first "entity( .. )" function is
called for a member of a class.

=back

=item Examples

 use VMTurbo;

 use VMTurbo qw(VirtualMachine Mem);

=back

=head1 TRANSPORTS

Scripts can use six different transports to connect to the server.

=over

=item Socket (default port = 8401)

Used to connect directly to the API servlet, and exchange requests and replies using a simple
POPD-like unencrypted text protocol. This is the fastest of the available transports. By default
it uses port 8401. Connection to this protocol may not be enabled on standard Appliances by default,
in which case the "pSocket" transport may be used instead.

=item pSocket (default port = 80)

The same as the "Socket", but is proxied via the on-appliance apache server. It uses port
80 by default.

=item SSL (default port = 8402)

This is the encrypted version of the Socket protocol. The encyption is implemented using the 
Secure Socket Layer. Encrypted connections are more secure, but imply a performance cost.
Uses port 8402 on the appliance, which may not be enabled on standard Appliances by default,
in which case see "pSSL" (below).

=item pSSL (default port = 80)

The proxied version of the SSL transport; uses the apache HTTPS port (443). Note that the use
of this protocol may require a patch to the perl SSL libraries. If you suspect this is the case
because the connection does not work, please refer to the following URL for details..

 https://rt.cpan.org/Public/Bug/Display.html?id=64054

=item HTTP (default port = 80)

Uses the HTTP protocol to exchange requests and replies. The HTTP protocol differs from the
Socket transport in that..

 - Commands can be invoked using URLs from web browsers, etc with HTTP.
 - Scripts using HTTP cannot use the "batch" function ("Socket" ones can).
 - Scripts using HTTP are restricted on the max request string length.
   Care must be taken not to allow the scripts to submit very large requests.
 - The HTTP protocol specifications allow the connection between client and
   server to be torn down by the client, server or interveening proxy. Servers
   and proxies typically do this frequently in order to prevent problems due
   to memory bloating. This can impact performance and functionality.
 - The HTTP protocol may be easier to proxy through some firewalls.

To connect directly to the applet, using port 8400 (which may not be enabled on standard
Appliances) or port 80 to proxy the connection via the on-board Apache server.

=item HTTPS (default port = 443)

The encrypted version of the HTTP protocol. This is the lowest performance transport option.

To connect directly to the applet, using port 9400 (which may not be enabled on standard
Appliances) or port 443 to proxy the connection via the on-board Apache server.

=back

=head1 OVERRIDEABLE AND PUBLIC ELEMENTS

The following global variables and functions are used by the API, and can be
inspected or modified by the client script.

=over 4

=item $VMTurbo::debug

Set this to a non-zero value to arrange that the default implementation
of VMTurbo::error(..) prints error messages to STDERR.

=item $VMTurbo::last_error

The string location where the default implementation of VMTurbo::error(..)
stores the most recently reported API error.

=item $VMTurbo::default_pSocket_proxy_to

The server and port to which pSocket transports are proxied. You should not
not normally change this.

=item $VMTurbo::default_pSSL_proxy_to

The server and port to which pSSL transports are proxied. You should not
not normally change this.

=item @VMTurbo::walkers

A list of perl modules that perform hash walks, and can cause problems with
recursive access to the server model. If the module is listed here, recursion
is prevented.

=item VMTurbo::error( $str )

This function is called by the API when errors are detected. The default implementation
stores the error in $VMTurbo::last_error and prints the error on STDERR if
$VMTurbo::debug is true.

You can implement your own version of this function in your script like this..

 sub VMTurbo::error {
    my ( $err ) = @_;
    ... do the work here ...
 }

=item $VMTurbo::xs

This is where the default implementaion of VMTurbo::parseXML(..) places the XML::Simple instance it uses.
You can use this variable to change the configuration of the parser.

=item VMTurbo::parseXML( $str )

This function is called by the API when XML strings needed to be decoded. See the
setXMLParser(..) function for more details.

=back

=head1 FUNCTIONS

The following package functions are supported:

=cut

#=============================================================================

=head2 batch

Sends a set of VMTurbo API commands to the server in a single message, and
collects the replies for all the commands executed in a single message. This can make
scripts for environments where there is high network latency between the client
and server significantly more performant.

The function is available for Socket, pSocket, SSL and pSSL transports only. It is
not supported for HTTP or HTTPS.

A maximum of 50 commands can be submitted in a single batch. Any commands after
the 50th will be executed, but their replies will not be returned to the client.
This maximum number can be configured by the administrator by changing the value
of "max_batch_size" in the relevant web.xml file.

The script can query the value of max_batch_size using the syntax..

 $size = $s->batch( "?max" );

=over 4

=item Syntax

 $replies = $s->batch(
     $command_1 => [ arguments-for-command-1 ],
     $command_2 => [ arguments-for-command-2 ],
     $command_3 => [ arguments-for-command-3 ],
     .... etc ....
 );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $command_N

Each command in the batch is identified by it's command name, which must be a string
containing one of the following keywords.. create, entityNameToUuid, exists, get,
getAttributes, getClasses, getEnum, getInstances, getOperations, getPackages, getReferences,
invoke, is, put, remove, pryClass.

=item arguments-for-command-N

The arguments to be passed to each command in the batch must be specified in a referenced
array, following the command name ($command_N). The suggested syntax is like this.

 get => [ $uuid, "displayName", "creationClassName" ]

This syntax allows for a reasonable level of script readablity.

The expected command arguments are those listed in this man page for the VMTurbo::Session
function with the same name. So for example;  the following are equivalent for all practical
purposes, except for the difference in returned reply format (see below for details).

 $reply = $s->get( $uuid, "displayName", "price" );
 $reply = $s->batch( get => [ $uuid, "displayName", "price" ] );

=back

=item Return

The function returns a reference to an array or null in the event of an error that prevents
the command batch from being submitted to the server.

The array contains the same number of entries as the number of commands submitted (up to a max
of 50 - or configured higher value). Each entry
contains the reply for the matching command. So $reply->[0] contains the reply for the first
command, $reply->[1] contains the reply to the second .. etc. Enties with "undef" values
indicate commands that failed (set $VMTurbo::debug to non-zero to see the corresponding
error messages).

=item Example

The following example creates two Mem objects, sets their display names, and passes back the
entity references to them.

 ($mem1, $mem2) = @{ $s->batch(
 	create => [ \"e", "Mem", displayName => "Memory #1" ],
	create => [ \"e", "Mem", displayName => "Memory #2" ]
 ) };

=item Notes

Not all the functions in the VMTurbo::Session package can be included in a command
batch; just those listed above (see the $command_N argument description).

When using the "invoke" function in a command batch, please note that like the "invoke"
command of VMTurbo::Session (but unlike the operation invocation logic of VMTurbo::Entity); it
does not support automatic disambiguation. Where the operation signature cannot be determined
from just the number of arguments and name of the operation, the operation ID should be used
in place of the name. See the description of the "invoke" function below for more details
on this point.

=back

=cut

my %batchable_cmds = (
	put => \&put,
	get => \&get,
	create => \&create,
	"exists" => \&exists,
	remove => \&remove,
	pryClass => \&pryClass,
	getAttributes => \&getAttributes,
	getClasses => \&getClasses,
	getEnum => \&getEnum,
	getInstances => \&getInstances,
	getOperations => \&getOperations,
	getPackages => \&getPackages,
	getReferences => \&getReferences,
	invoke => \&invoke,
	is => \&is,
	entityNameToUuid => \&entityNameToUuid
);

sub batch {
	local $func = "batch";
	my $this = shift @_;

	if ( "@_" eq "?max" ) { return $this->_call("?max_batch_size"); }

	local $this->{_batch_flag} = 1;

	if (@_ == 0) {
		VMTurbo::error( "$func - Empty action list" );
		return undef;
	}

	if ((@_ % 2) != 0) {
		VMTurbo::error( "$func - Needs even number of arguments" );
		return undef;
	}

	unless ($this->{connection}->{transport}->can("_send_requests")) {
		VMTurbo::error( "$func - Not implemented for transport \"$this->{connection}->{tsp_name}\"" );
		return undef;
	}

	my @cmds = ( );
	while (@_) {
		my $cmd = shift @_;
		my $args = shift @_;
		my $fn = $batchable_cmds{$cmd};
		unless (defined $fn) {
			VMTurbo::error( "$func - Function '$cmd' not batchable" );
			return undef;
		}
		unless (ref($args) eq "ARRAY") {
			VMTurbo::error( "$func - Arguments for '$cmd' must be an array reference" );
			return undef;
		}
		my $cmdpkt = &{$fn}( $this, @{$args} );
		return undef unless defined $cmdpkt;
		$cmdpkt =~ s/\n$//s;
		push @cmds, (@_ > 0 ? "@" : "!").$cmdpkt;
	}
	my $rtn = $this->{connection}->_send_requests( @cmds );

	my $reply =  $this->{connection}->_read_reply( );
	return undef unless defined $reply;
	return $this->_render( $reply );
}


#-----------------------------------------------------------------------------

=head2 close

Closes the connection with the server.

=over 4

=item Syntax

 $s->close( );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=back

=item See Also

"new" and "isConnected" functions.

=back

=cut

sub close {
	local $func = "close";
	my ( $this, @args ) = @_;
	return $this->{connection}->_close(@args);
}

#-----------------------------------------------------------------------------

=head2 create

Creates a new entity of the specified class, and optionally sets one of more
attributes or references for the newly created object.

=over 4

=item Syntax

 $uuid_or_entity_ref = $s->create( [\$options,] $class_name[, %attribute_values ]);

=item Where

=over 4

=item $options

Is a string containing one or more option letters.

=over 4

=item k   (keep)

Option 'k' indicates that the newly created object should not be deleted if any of
the specified attributes cannot be set.

=item f   (force)

Option "f" can be used to force the creation of the entity even if a name is
specified, and it duplicates the name of an existing entity.
Without this option, an attempt to create an entity with the same
name as one that exists already causes an error to be returned, and no new
object is created.

=item e   (entity)

Option "e" can be used to cause "create" to return an entity reference
(the default mode of operation).

=item u   (uuid)

Option "u" can used to cause "create" to return the Uuid of the create entity
instead of an entity reference.

=back

=item $class_name

The name of the class for which a new member is to be created.

=item %attribute_values

After the $class_name can come a set of "attribute => $value" pairs that can
be used to set the attributes and/or references in the newly created entity.
These attributes are set using perl's "hash" population syntax.

To unset an attribute, pass "undef" as the value.

When setting a reference, you have a number of choices:

=over 4

=item Setting a singleton reference

If the reference is a singleton (points to just one entity) then you give the UUID, name or
perl reference of the entity object as the value.

=item Emptying a singleton reference

To indicate that a singleton reference should refer to no entities, pass an empty
string or undef as the value.

=item Setting a list reference

If the reference can take multiple values then you should supply a reference to
an array that contains the UUIDs, names or perl object references of the entities to be
placed in the entity reference.

=item Emptying a list reference

If you want to indicate that a list reference should contain no entities, pass
an empty array as the value.

=back

=back

=item Return

Returns the UUID of the created entity (if option "u" is specified)

or: Returns the entity reference for the created entity (if option "e" is specified)

or: Returns undef in the event of an error.

=item Notes

A call to "create" with a set of "attribute=>$value" specifications is implemented as
a single exchange with the server, and so can provide significantly better performance than
calling "create" followed by a set of separate "put" calls, particularly with a server or
network that is experiencing high load.

If the setting of an attribute fails, the call will delete the newly created entity. If the
option "k" is specified then this will not happen - meaning the command will potentially be
partially complete - ie: the entity will have been created but not all the attributes will
have been populated. Note that if the object needs to be deleted, it will be removed
using a "non-recursive" removal.

=item Examples

 $mem1 = $s->create( "Mem", name=>"Test memory object" );
 $mem2 = $s->create( \"f", "Mem", name=>"Test memory object" );
 $mem3 = $s->create( \"e", "Mem" );
 $vm2  = $s->create(
               "VirtualMachine",
               name        => "vm2",
               displayName => "second VM",
	       ComposedOf  => [ $mem1, $mem2 ]
 );
 $vm3ref = $s->create( \"e",
               "VirtualMachine",
               name        => "vm3",
               displayName => "Third VM"
 );

=back

=cut

sub create {
	local $func = "create";
	return _put_or_create( @_ );
}

#------------------------------------------------------------------------------

=head2 debug

Turns various debugging options on or off.

=over 4

=item Syntax

 $s->debug( $flag );
 $s->debug( $type => $flag [, $type => $flag ...] )

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $type

In the second of the two possible syntaxes above, this is letter that indicates
what type of debugging is to be controlled. The possible values are.

=over 4

=item s   (session)

Used to control debugging of the session.

=item c   (connection)

Used to control debugging of the connection that the session is using.

=item t   (transport)

Used to control debugging of the connection transport.

=back

The first of the two possible syntaxes (where a "type" is not used) changes the debugging
at all three levels at the same time.

=item $flag

A boolean value that indicates whether debugging is to be turned on (1) or off (0).

=back

=item Examples

 $s->debug(1);
 $s->debug(t=>1, c=>0);

=item Note

Change the value stored in $VMTurbo::debug to turn error reporting from VMTurbo::error()
on or off.

=back

=cut

sub debug {
	local $func = "debug";
	my $this = shift @_;
	my %flags = ( );
	if (@_ == 1) {
		my $flag = shift @_;
		$flags{t} = $flags{c} = $flags{s} = $flag;
	} else {
		%flags = @_;
	}
	eval {
		$this->{debug} = $flags{s};
		$this->{connection}->{debug} = $flags{c};
		$this->{connection}->{transport}->{debug} = $flags{t};
	};
}

#------------------------------------------------------------------------------

=head2 entity

Obtains a reference to an entity in the server repository, by which entity
attributes, references and operations can be accessed in the perl script.

=over 4

=item Syntax

 $e = $s->entity( $class, $uuid );
 $e = $s->entity( undef, $uuid );
 $e = $s->entity( $class, undef );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $class

The name of the class of which the entity is a member. If the class name is not known,
undef can be used instead. In this latter case, the client API library performs a query
against the server to determine the class name - and so there is an additional
client/server exchange required and for this reason it is recommended to specify the
class name whenever possible.

=item $uuid

The UUID or name of the object for which a reference is required. If the class is a singleton
and the $uuid argument is undef, then a reference to the single member of the class is
returned. It is an error to use an undef $uuid if the class has no members, or more
than one member.

=back

=item Return

The entity reference or undef (in the event of an error).

=item Example

 $mem1 = $s->entity( "Mem", $uuid );
 $rr = $s->entity( "RepositoryRegistry", undef );

=back

=cut

sub entity {
	local $func = "entity";
	if ( @_ != 3 ) {
		VMTurbo::error( "$func - wrong number of arguments" );
		return undef;
	}
	my ( $this, $class, $uuid ) = @_;

	if (defined($uuid) && !defined($class)) {
		$class = $this->_call( "get", $uuid, "creationClassName" );
		return undef unless (defined $class);
	}
	return undef unless $this->loadClass( $class );
	return VMTurbo::Entity->_new( $this, $class, $uuid );
}

#------------------------------------------------------------------------------

=head2 entityNameToUuid

Searches the server repository for an entity with a specified name, and returns
its uuid if found.

=over 4

=item Syntax

 $uuid = $s->entityNameToUuid( $name );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $name

The name of the entity whose uuid is wanted.

=back

=item Return

Returns the entity uuid or undef if in the event of an error (such as: name not
found). Note that if more than one entity exists with the same name, only one of
them is returned, for this reason the call is only appropriate for use with
entities with unique names.

=item Example

 $vm1 = $s->entityNameToUuid( "T-VM01" );

=item Notes

This function is simply a call to the "getObject()" operation of the RepositoryRegistry class.

=back

=cut

sub entityNameToUuid {
	local $func = "entityNameToUuid";
	my ( $this, $name ) = @_;
	return $this->_call( "inv.cu", "RepositoryRegistry", "getObject", $name );
}

#-----------------------------------------------------------------------------

=head2 exists

Tests whether a given entity exists.

=over 4

=item Syntax

 if ( $s->exists( $entity_ref ) ) { .... }
 if ( $s->exists( $uuid_or_name ) ) { .... }

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $entity_ref

A reference to the entity who's existance is to be tested. The function checks that this
entity exists in the server AND that the class to which it belongs matches the class
specified when the entity reference was initialised in the script.

=item $uuid_or_uuid

The Uuid or name of the entity who's existance is to be checked.

=back

=item Return

The function returns a boolean value that indicates whether the entity exists and (if the
argument was an entity reference) that the specified class matches with the server.

An undef value is returned if the function cannot be executed.

=item Example

 $e = $s->entity( "VirtualMachine", "VM-0001" );
 unless ($s->exists( $e )) { print "VM-0001 doesnt exist, or isnt a VM\n"; }

=back

=cut

sub exists {
	local $func = "exists";
	if (@_ != 2) {
		VMTurbo::error("$func - wrong number of arguments");
		return undef;
	}
	my ( $this, $uuid ) = @_;
	if (ref($uuid) =~ m/^VMTurbo::Entity::/ && $uuid->{-singleton}) {
		return $this->_call( "exi.c", $uuid->{-class} );
	} elsif (ref($uuid) =~ m/^VMTurbo::Entity::/ && !$uuid->{-singleton}) {
		return $this->_call( "exi", $uuid->{-class}, $uuid->{-uuid} );
	} else {
		return $this->_call( "exi", $uuid );
	}
}

#-----------------------------------------------------------------------------

=head2 get

Gets the value of one or more attributes (or references) for an entity.

There are two types of syntax that can be used; invoking the "get" function of
the VMTurbo::Session package, or using the tied hash synax of the VMTurbo::Entity
package.

=over 4

=item Syntax

 $value = $s->get( [\$options,] $entity, $attr );

 $values_ref = $s->get( [\$options,] $entity );

 $values_ref = $s->get( [\$options,] $entity, $attr1, $attr2, ... );

or (using an entity reference context)

 $value = $entity_ref->{$attr};

 $values_ref = $entity_ref->{"*"};

 $attr_names = [ $attr1, $attr2, ...  ];
 $values_ref = $entity_ref->{ $attr_names };

 $values_ref = $entity_ref->{ "$attr1 $attr2 ..." };

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $options

A string containing one or more of..

=over

=item e   (entity)

When the return includes entity Uuids, replace them with entity references, created
using VMTurbo::Session::entity. This is the default.

=item m   (multiple)

Format the return as for the "multiple attribute" variant, even if only one attribute
is requested. This forces the return to be a reference to a hash, regardless of how
many values it contains.

=item n   (name)

When the return includes entities, return their names instead of their uuids. Note that
it is possible and valid for an entity's name to undefined - in which case an undef
is returned.

=item u   (uuid)

Any entities referred to in the return are given as their Uuids.

=item v   (verbose)

When entities are referred to in the return, they are given as references to sub-arrays
each of which contains 4 elements. These are; classname, uuid, name and displayName.

=back

Note that the options "e", "n", "u" and "v" are mutually exclusive and only one
may be specified.

The options "e", "n", "u" and "v" can either be specified as the first argument to the
get function (as shown above) OR appended to the attribute name with a delimiting dot,
like this..

 $value = $entity->{"purchasedBy.u"};

If this latter syntax is used, then different formatting options can be used for each
of the requested attributes.

=item $entity

The Uuid, name or reference for the entity to be inspected.

=item $entity_ref

The perl reference to the entity to be inspected - as returned by the "entity" function
or a "get" operation with the "e" option specified.

=item $attr, $attr1, $attr2, $attr_names

The name[s] of the attribute[s] or reference[s] to get (possibly tagged with an option
letter).

=back

=item Return

If one name is specified and the \"m" option is NOT used, then the return is the value of
that element. For a multi-element reference, this will be a perl reference to an array of
uuids (or other formats - depending on the option chosen).

If multiple names are specified or the \"m" option is used, then the return is a perl reference
to a hash containing a name=>value pair for each of the requested elements.

If no names are specified, or the name "*" is given then all available attributes and references
for the entity are returned as a hash.

=item Examples

 foreach my $vm (@{ $s->getInstances("VirtualMachine") }) {
     print $s->get($vm, "displayName")."\n";
 }

 $info = $vm1->{["name", "HostedBy.e"]};
 print "VM Name = ".$info->{name}.", Hosted By "
            .$info->{HostedBy}->{displayName}."\n";

 $info = $vm1->{"name HostedBy.u"};

=back

=cut

sub get {
	local $func = "get";
	my $this = shift @_;
	my $options = "";
	if (@_ > 0 && ref($_[0]) eq "SCALAR") {
		$options = ".".${shift @_};
	}
	my $uuid = shift @_;
	if (!defined $uuid) {
		VMTurbo::error( "$func - Missing UUID argument" );
		return 0;
	}
	if (ref($uuid) =~ m/^VMTurbo::Entity::/) {
		if ($uuid->{-singleton}) {
			$uuid = $uuid->{-class};
			if ($options !~ /c/) { $options .= ($options eq "" ? "." : "")."c"; }
		} else {
			$uuid = $uuid->{-uuid};
		}
	}
	if ( @_ == 0 ) { push @_, "*"; }
	return $this->_call("get$options", $uuid, @_);
}

#------------------------------------------------------------------------------

=head2 getAttributes

Gets a list of the attribute names for a specified class. The returned list includes
attributes declared in the specified class AND the parent classes it inherits from.

=over 4

=item Syntax

 $attr_list_ref = $s->getAttributes( $class_name );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $class_name

The name of the class whose attributes are to be collected.

=back

=item Return

Returns a perl reference to an array containing the attribute names, or
undef in the event of an error.

=item Example

 @attr = @{ $s->getAttributes( "VirtualMachine" ) };

=item See Also

getReferences() and getProperties()

=back

=cut

sub getAttributes {
	local $func = "getAttributes";
	my $this = shift @_;
	return $this->_getProperties( \"Aa", @_ );
}

#------------------------------------------------------------------------------

=head2 getClasses

Get the list of classes defined in the server repository, or the class branch
starting at a specified base.

=over 4

=item Syntax

 $classes = $s->getClasses( [$base_class_name] );
 $classes = $s->getClasses( \$option[, $base_class_name] );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $option

is one of ..

=over 4

=item s   (sort)

to sort the output

=item t   (tree)

to give an indented tree output (one large string)

=back

=item $base_class_name

The name of the class to use as the base (root) of the classes to
be returned. If specified, then the class and its descendants are returned. If not
specified, the all classes known (and exposed) by the server are returned.

=back

=item Return

Returns a reference to an array which contains the names of the classes, or (if option
't' is specified) a single string that contains an indented class tree
document.

undef is returned in the event of an error.

=item Example

 $classes = $s->getClasses( \"s", "Actor" );
 foreach my $c ( @{ $classes } ) { print "$c\n"; }

=back

=cut

sub getClasses {
	local $func = "getClasses";
	my $this = shift;
	my $options = "";
	my $base = undef;
	if (@_ > 0 && ref($_[0]) eq "SCALAR") { $options = "." . ${shift;} }
	if (@_ > 0) { $base = shift; }
	if (@_ != 0) {
		VMTurbo::error("$func - too many arguments");
		return undef;
	}

	if (defined $base) {
		return $this->_call( "getc$options", $base );
	} else {
		return $this->_call( "getc$options");
	}
}

#-----------------------------------------------------------------------------

=head2 getEnum

Collect information about enumerations.

=over 4

=item Syntax

 $enum_list_ref = $s->getEnum( [\$options] );
 $value_list_ref = $s->getEnum( [\$options,] $enumeration_name );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $options

=over 4

=item s   (sort)

indicates that the results should be sorted. It is recommended to use the perl "sort"
function rather than this option - in order to avoid the additional minor performance
load on the server.

=back

=item $enumeration_name

The name of an enumerated type whose possible values should be returned.

=back

=item Return

If no $enumeration_name is specified, the list of defined enumeration types
is returned.

If a $enumeration_name is specified, the list of possible values for the specified
enumeration type is returned.

In the event of an error, undef is returned.

=item Example

 foreach $e ( sort @{ $s->getEnum( ) } ) {
    print "$e\n";
    foreach $val ( sort @{ $s->getEnum( $e) } ) 
       print "   $val\n";
    }
 }

=back

=cut

sub getEnum {
	my ( $this, $name, $empty ) = @_;
	local $func = "getEnum";
	if (defined $empty) {
		VMTurbo::error("$func - Too many arguments");
		return undef;
	}
	unless (defined($this)) {
		VMTurbo::error("$func - Missing argument[s]");
		return undef;
	}
	if (defined $name) {
		return $this->_call("geten", $name);
	} else {
		return $this->_call("geten");
	}
}

#------------------------------------------------------------------------------

=head2 getInstances

Returns the instances of a specified class. The return can be by Entity reference, Uuid,
or name. The returned list can either include members of the class and its children
or just the members of the class itself; depending on the specified options.

=over 4

=item Syntax

 $instances = $s->getInstances( [ \$options, ] $class_name[, $market_name] );

=item Where

=over 4

=item $options

an optional string, containing one or more of..

=over 4

=item e   (entity)

return instance entity references (the default).

=item n   (names)

return instance names.

=item u   (uuid)

return instance uuids.

=item v   (verbose)

return uuids, types, names and displayNames in an array of arrays.

=item l   (leaf)

return "leaf" instances only (exclude members of child classes).

=back

Note: "e", "n", "u" and "v" are mutually exclusive. Results are undefined if more
than one of these are used in the same call.

=item $class_name

is the name of the class whose members are to be returned.

=item $market_name

when getting the members of an "Actor" class, the $market_name attribute can be
used to limit the returned list to objects that participates in the named
market.

=back

=item Return

Returns a reference to an array containing the instances, or undef in the
event of an error.

=item Example

 $instances = $s->getInstances( \"n", "VirtualMachine" );
 foreach $vm ( @{ $instances } ) {
    print "$vm\n";
 }

=back

=cut

sub getInstances {
	local $func = "getInstances";
	my $this = shift @_;
	my $arg1 = shift @_;
	my ($option, $class);
	if (ref($arg1) eq "SCALAR") {
		$option = ".$$arg1";
		$class = shift @_;
	} else {
		$option = "";
		$class = $arg1;
	}
	my $market = @_ > 0 ? $_[0] : undef;

	unless (defined($this) && defined($class)) {
		VMTurbo::error("$func - Missing argument[s]");
		return undef;
	}
	if (defined $market) {
		return $this->_call( "geti$option", $class, $market );
	} else {
		return $this->_call( "geti$option", $class );
	}
}

#-----------------------------------------------------------------------------

=head2 getOperations

Returns a list of the operations supported by a named class, complete with
information about their return and argument types.

=over 4

=item Syntax

 $operations = $s->getOperations( [\$options,] $class );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $class

The name of the class whose operation details are to be collected. If $class is
"*", then the list of operations from all classes is returned, broken down by
class. This latter option is designed to provide developer documentation.

=item $options

a string (default is "n") that optionally includes one or more of..

=over 4

=item a   (all)

list all operations. Without this option, you only
get the operations directly declared in the class
itself, not those defined in its parent classes.

=item j   (java)

use more "java-like" data type names. Without this option, you get the API's
internal contract types where available.

=item n   (names)

include argument names - not just their types.

=back

=back

=item Return

This function returns the list of operations for the named class, with the
format and content modified by the specified options. The output consists of
a single large string, each line of which looks like this by default..

 131 d subtract( d, b )

The leading number is the API's internal ID for this operation and has no
significance to most scripts. This number is used internally by the API in
its disambiguation logic. See also "getOperationID( .. )" and "invoke( .. )".

The second field is the contracted form of the return type. "d" in this case
signifies a "double".

Next is the name of the operation, which is followed by the list of argument
types in brackets. These types are also "contracted" to short forms.

Where a return or argument type is a class, it is shown in the output as the letter
"u" (means: uuid) followed by a colon and the class name, like this: "u:Mem".

Where a return or argument is an enumerated type, it is show in the output as the
letter "e" followed by a colon and the enumeration name, like this: "e:Severity".

With the option "q" specified, the output is further contracted to exclude the
class and enumeration types. These are simply shown by the letters "u" and "e" without
the colon and full name.

With the option "n" specified, the output is modified to include the argument names
like this..

 131 d subtract( d amount, b isPercentage )

With the option "j" specified, the output uses more java-like type names, like this..

 131 Double subtract( Double, Boolean )

If you wish to split the output into an array of lines, then use the perl "split" function,
like this..

 $operations = $s->getOperations(\"a", "Mem");
 if (defined $operations) {
     @operations = split(/\r?\n/, @{$operations} );
 }

=item Example

 $operations = $s->getOperations( \"a", "Mem" );

=back

=cut

sub getOperations {
	local $func = "getOperations";
	my $this = shift @_;
	my $arg1 = shift @_;
	my ($options, $class);
	if (ref($arg1) eq "SCALAR") {
		$options = ".$$arg1";
		$class = shift @_;
	} else {
		$options = "";
		$class = $arg1;
	}
	unless (defined($this) && defined($class)) {
		VMTurbo::error("$func - Missing argument[s]");
		return undef;
	}
	return $this->_call( "getop$options", $class );
}

#------------------------------------------------------------------------------

=head2 getOperationID

Gets the numeric ID for a specified operation, with the specified argument types.

=over 4

=item Syntax

 my $id = $s->getOperationID( $class, $operation, $arguments );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $class

The name of the class. Eg "VirtualMachine"

=item $operation

The name of the class operation. Eg: "buyDynamicCommodity"

=item $arguments

A string that denotes the types of the arguments that the function takes. Each character
of the string gives the type of one argument. Each character is a lower case letter chosen
from the following..

=over 4

=item a - class Attribute or reference name

=item b - Boolean

=item c - Class name

=item d - Double float

=item e - Enumeration

=item i - Integer

=item s - String

=item u - UUID (or object entity)

=item v - Void

=back

=back

=item Return

This function returns the numeric operation signature ID or undef (null) if no
matching signature can be found. The ID can be used as the 2nd argument of the
"invoke" function of the VMTurbo::Session package.

=item Example

 $id = $s->getOperationID( "VirtualMachine", "buyDynamicCommodity", "csd" );
 $commodity = $s->invoke( $vmref, $id, $type, $key, $consumption );

=item Note

The technique of invoking an operation by it's ID is only needed if the operation cannot
uniquely be identified on the basis of it's name and number of arguments. The more normal
approach is to invoke operations by name, like this..

 $commodity = $s->invoke( $vmref, "buyDynamicCommodity", $type, $key, $consumption );

or:

 $commodity = $vmref->buyDynamicCommodity( $type, $key, $consumption );

=back

=cut

sub getOperationID {
	my ( $this, $class, $opname, $argtypes ) = @_;

	return undef unless $this->loadClass( $class );

	unless (
		defined $this
		&& defined( $this->{opmap} )
		&& defined( $this->{opmap}->{$class} )
		&& defined( $this->{opmap}->{$class}->{$opname} )
		&& defined( $this->{opmap}->{$class}->{$opname}->{$argtypes} )
		&& ref( $this->{opmap}->{$class}->{$opname}->{$argtypes} ) eq "ARRAY"
		&& @{$this->{opmap}->{$class}->{$opname}->{$argtypes}} != 1
	) {
		VMTurbo::error( "Cant find operation signature for '$class\::$opname \{$argtypes}'" );
		return undef;
	}

	return {$this->{opmap}->{$class}->{$opname}->{$argtypes}->[0]};
}

#------------------------------------------------------------------------------

=head2 getPackages

Get the list of packages implemented in the server.

=over 4

=item Syntax

 $packages = $s->getPackages( );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=back

=item Return

Returns a perl reference to an array containing the list of package names.

=back

=cut

sub getPackages {
	local $func = "getPackages";
	my ( $this, $empty ) = @_;
	if (defined $empty) {
		VMTurbo::error("$func - Too many arguments");
		return undef;
	}
	unless (defined($this)) {
		VMTurbo::error("$func - Missing argument[s]");
		return undef;
	}
	return $this->_call( "getpa" );
}

#------------------------------------------------------------------------------

=head2 getPeerCertificate

Gets the X509 peer certificate object for the underlying encryption layer. For
unencrypted session types, this returns undef.

=over 4

=item Syntax

 $cert = $s->getPeerCertificate();

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=back

=item Return

For SSL, pSSL and HTTPS connections, this function returns a reference to the peer certificate
object. For Socket, pSocket and HTTP connections it returns undef. The returned reference can
be used as the context to call "subject_name()" or "issuer_name()" from.

=item Example

 $cert = $s->getPeerContext();
 if (defined $cert) {
     print "Subject = ".$cert->subject_name()."\n";
     print "Issuer  = ".$cert->issuer_name()."\n";
 }

=back

=cut

sub getPeerCertificate {
	local $func = "getPeerCertificate";

	my ( $this ) = @_;
	my $cert = $this->{connection}->{transport}->{peercert};
	if (defined $cert) { return $cert; }
	$this->_call("noop");
	return $this->{connection}->{transport}->{peercert};
}

#------------------------------------------------------------------------------

=head2 getProperties

Obtains information about the properties of a class.

=over 4

=item Syntax

 $props = $s->getProperties( [\$options, ] $classname );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $options

An optional strings composed of one of more of the following characters..

=over 4

=item a   (all)

Causes the function to work with the properties of the class AND the classes from
which it inherits. Without this option, only the properties defined directly for
the class itself are returned.

=item A   (attributes)

Limits the output to show attribites only.

=item R   (references)

Limits the output to show references only.

=item v   (verbose)

Cause the function to return more than just the property names. If the option is NOT
specified, the return is a just a list of the property names. If the option IS specified the
return is a list of sublists. Each sublist contains the following information for each
property of the class..

=over 4

=item 0: Property name

The name of the property (attribute or reference).

=item 1: Property Type

The string 'ref' if the property is a reference, or 'attr' if it is an attribute.

=item 2: Value Type

For references, this contains the name of the class to which the reference refers. For attributes
it contains the name ot the type of data the attribute can contain. The returned type is the full type
name, not the sortened one-character (etc) form - so you get "String" rather than "s".

=item 3: Lower bound

The upper bound for the property.

=item 4: Upper bound

The upper bound for the property.

=item 5: The feature ID

The internal numeric ID by which the property is known to the server. 

=item 6: Containing class

The name of the class in which the property was defined.

=item 7: Flags

A list of one-character flags that give further information about the property. These are
typically attributes of the property, defined in the model.

=over 4

=item C - property is Changeable

=item D - property is Derived

=item M - property can contain Many elements

=item O - property is Ordered

=item Q - property is uniQue

=item R - property is Required

=item T - property is Transient

=item U - property can be Unset

=item V - property is Volatile

=back

=back

=item s   (sort)

Sort the results into property name order. 

=back

=item $classname

The name of class who's properties are to be returned.

=back

=item Return

This function returns the list of properties for the named class, modified according
to the characters specified in the option string. undef is returned in the event of
an error.

The list is an array of strings unless option "v" is specified, in which casee the
return is an array of subarrays.

=item Example

The following fragment lists the properties of the "Mem" class, and idicates
for each whether or not if can be modified.

 my $class = "Mem";
 foreach my $prop ( @{ $s->getProperties( \"avs", $class ) } ) {
     print $prop->[0]." ".($prop->[7] =~ /C/ ? "can" : "cannot")." be changed\n";
 }

=back

=cut

sub _getProperties {
	my $this = shift @_;
	my $options = "";
	if (@_ > 0 && ref($_[0]) eq "SCALAR") {
		$options = ".".${shift @_};
	}
	if (@_ != 1) {
		VMTurbo::error( "$func - wrong number of arguments");
		return undef;
	}
	$class = shift @_;
	return $this->_call( "getpr$options", $class );
}

sub getProperties {
	local $func = "getProperties";
	return _getProperties( @_ );
}

#------------------------------------------------------------------------------

=head2 getReferences

Get a list of the references defined for a specified class. A "reference" is an
attribute that contains a link to one or more other entities in the model.

=over 4

=item Syntax

 $references = $s->getReferences( $class_name );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $class_name

The name of the class whose reference names are to be collected.

=back

=item Return

Returns a perl reference to an array containing the names of the defined references
for the specified class.

Or: returns undef in the event of an error.

=item Example

 @ref_list = @{ $s->getReferences( "Mem" ) };

=item See Also

getAttributes() and getProperties()

=back

=cut

sub getReferences {
	local $func = "getReferences";
	my $this = shift @_;
	return $this->_getProperties( \"Ra", @_ );
}

#------------------------------------------------------------------------------

=head2 invoke

Invoke an operation for an entity or singleton class in the model.

=over 4

=item Syntax

 $values = $s->invoke( [ \$options, ] $entity, $operationName [, @arguments] );
 $values = $entity_ref->operationName( [ \$options, ] @arguments );
 $values = $entity_ref->operationName_argTypes( [ \$options, ] @arguments );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $entity

The UUID, name or reference of the entity whose operation will be called,
or the name of a singleton class (one with only one member). If a
class name is used, the $options MUST include the letter "c" (first syntax
above).

=item $entity_ref

The perl reference for the entity whose operation is to be called.

=item $operationName, operationName( .. )

In the first syntax shown above, this is the name or ID number of the operation to
be called. The operation ID number is given in the output of getOperations() or by the
getOperationID() function, but most scripts do not need to use the numeric form since
the majority of class operations are not defined ambiguously (from perl's perspective).
Please note that operation numbers are NOT guaranteed to be consistently the
same for each session or server version.

In the second and third syntax, this is the name of the operation to be called, and may optionally
be tagged with argument types to disambiguate it.

=item argTypes

A list of argument type letters can be included in the operation name to assist with
disabiguating two operations with the same name and same number of arguments. For example
the following invokes the getEReference operation with two arguments, while indicating
that the arguments are both strings.

 $result = $ent->getEReference_ss( $class, $refname );

This style of "disambiguated" invokation is rarely required since the majority of operations
can be uniquely defined in terms of their names and number of arguments alone.

=item @arguments

List of scalar arguments to be passed to the operation.

=item $options

A string containing one or more of the following..

=over 4

=item c   (class)

the $entity is to be interpreted as a singleton class name (first syntax only).

=item e   (entity)

entities listed in output are returned as entity references, as if created
using VMTurbo::Session::entity.

=item n   (name)

entities listed in output are to be returned as names

=item u   (uuid)

entities listed in output are to be returned as UUIDs

=item v   (verbose)

entities listed in output are returned as "class, uuid, name and displayname" in
an array of subarrays.

=back

Note: "e", "n", "u" and "v" are mutually exclusive. Results are
undefined if more than one of these are used in the same call.

=back

=item Return

Returns the value returned by the operation, or undef in the event of
a failure (such as a thrown java exception). Operations that are declared as returning void,
actually return the number 1 to the perl script - this allows the script to determine
the difference between an error and a successful execution.

=item Example

 my $info = $s->invoke( \"c", "TopologyService", "getAllNames", ".*", ".*" );

 my $ts = $s->entity( "TopologyService", undef );
 my $info = $ts->getAllNames( ".*", ".*" );

 my $ts = $s->entity( "TopologyService", undef );
 my $opid = $s->getOperationID( "ToplogyService", "getAllNames", "ss" );
 my $info = $s->invoke( $ts, $opid, ".*", ".*" );

=back

=cut

sub invoke {
	my $this = shift @_;
	my $arg1 = shift @_;
	my ($option, $obj, $operation, @args);
	if (ref($arg1) eq "SCALAR") {
		$option = ".$$arg1";
		($obj, $operation, @args) = @_;
	} else {
		$option = "";
		$obj = $arg1;
		($operation, @args) = @_;
	}
	local $func = "invoke";
	unless (defined($this) && defined($obj) && defined($operation)) {
		VMTurbo::error("$func - Missing argument[s]");
		return undef;
	}
	
	return $this->_call("inv$option", $obj, $operation, @args );
}

#------------------------------------------------------------------------------

=head2 is

Test whether a specified entity is a member or decendant of a given class.

=over 4

=item Syntax

 $boolean = $s->is( $entity, $class );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $entity

The UUID, name or reference of the entity whose class membership is to be tested.

=item $class

The name of class against which the entity is to be checked.

=back

=item Return

This function returns true if the entity is a member of the specified class, or of a
class inherited from the specified one. Or, to put it another way: it answers the
question: "is this entity descended from this class?".

=item Example

 $vm = $s->entity( "VirtulMachine", "T-VM01" );
 if ($s->is( $vm, "Actor" )) {
     print "The VM is an actor (as expected)\n";
 }

=back

=cut

sub is {
	local $func = "is";
	if (@_ != 3) {
		VMTurbo::error("$func - wrong number of arguments");
		return undef;
	}
	my ( $this, $uuid, $class ) = @_;

	return $this->_call("is", $uuid, $class);
}

#------------------------------------------------------------------------------

=head2 isConnected

Tests whether the connection between the client and server is seen by the
system as active.

Take care: there are technical situations in which the client library is not
aware of a break in the transport - particularly for the HTTP and HTTPS transports
which break and rebuild the connection transparently and frequently.

=over 4

=item Syntax

 $ok = $s->isConnected( );

=item Where

=over 4

=item $s

A reference to the session to the server to be tested, established using VMTurbo::Session::new.

=back

=item Return

This function returns true if the connection is still flagged as active by the client OS and perl
interpreter. Note the disclaimer in that statement! The function can only report on the state
returned to it by the OS and Perl run time - and there appear to be situations in which this
information cant be relied on.

The function returns false is the connection is closed or broken (this *can* be believed).

undef is returned if the function call fails.

=item Example

 unless ($s->isConnected()) {
    print "Connection with the server lost\n";
 }

=back

=cut

sub isConnected {
	my ( $this ) = @_;

	return $this->{connection}->_isConnected( );
}

#------------------------------------------------------------------------------

sub loadClass {
	my ($this, $class) = @_;
	return 1 if (exists $this->{opmap}->{$class});
	my $ops = $this->_call( "getop.aq", $class );
	return 0 unless defined $ops;
	foreach my $op (split(/\r?\n/, $ops)) {
		if ($op =~ m/^(\d+) (.*?) (.*?)\((.*?)\)$/) {
			my $id = $1;
			my $rtn = $2;
			my $name = $3;
			my $args = $4;

			$args =~ s/ //g;
			my @args = split(/,/, $args);
			my $argnum = 0+@args;

			my @simple_args = @args;
			foreach (@simple_args) { $_ =~ s/:.*//; }
			my $sargs = join("", @simple_args );

			push @{$this->{opmap}->{$class}->{$name}->{$argnum}}, [ $id, $rtn, @args ];
			push @{$this->{opmap}->{$class}->{$id}->{$argnum}}, [ $id, $rtn, @args ];
			push @{$this->{opmap}->{$class}->{$name}->{$sargs}}, $id;

			unless (defined( &{"VMTurbo\::Entity\::$class\::$name"})) {
				eval "sub VMTurbo\::Entity\::$class\::$name { return VMTurbo\::Entity\::_invoke_op_name( \"$name\", \@_ ); }";
			}
			eval "sub VMTurbo\::Entity\::$class\::${name}_${sargs} { return VMTurbo\::Entity\::_invoke_op_id( $id, $argnum, \@_ ); }\n";
		}
		eval "\@VMTurbo\::Entity\::$class\::ISA = ( \"VMTurbo::Entity\" )";
	}
	return 1;
}

#------------------------------------------------------------------------------

=head2 login

Defines the user name and password to be used to log in to the server. For the
Socket and SSL protocols, this results in an immediate attempt to log in, and
the call will fail if the wrong values are given. For the HTTP and HTTPS protocols,
the values are remembered and used in the next transaction. Incorrect name or
password are reported as a failure in the next transaction, not in the "Login"
command itself.

This command is not needed (indeed: should not be used) if the argument provided
to the "new( .. )" function was a URL, and included a username and password.

=over 4

=item Syntax

$ok = $s->login( [\$option,] $user, $password );

=item Where

=over 4

=item $option

An option string containing:

=over 4

=item t  (test)

For HTTP and HTTPS transports, this option forces the API to test the connection
immediately with the supplied user name and password in order to determine whether they are
correct. For Socket and SSL transports the option has no effect because the
result of the login attempt is known immediately anyway.

=back

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $user

The name of the user.

=item $password

The password to be used.

=back

=item Return

A boolean value that indicates whether the login was successful.

For HTTP and HTTPS, the call returns true (1) regardless of whether
the username and password are correct. Incorrect details will be detected on the
next transaction to be attempted unless the option "t" is provided, in which case
the API will attempt a dummy command exchange for you, and return false if it
failed.

Note that only one log in attempt is permitted per Socket or SSL connection;
if the login fails then the connection must be closed and re-opened in order to retry.

=item Note

If a list of class was specified in the "use VMTurbo" statement, then the operation templates
for those classes will be loaded (if possible). If no classes were listed, then the templates
are loaded instead on an "as needed" basis.

=item Example

 if (!$s->login("operator", "password")) {
   print "Cant log in\n";
 }

=back

=cut

sub login {
	my $this = shift @_;
	my $option = "";
	if (@_ > 0 && ref($_[0]) eq "SCALAR") {
		$option = ${shift @_};
	}
	my ( $user, $password ) = @_;

	unless ( $this->{connection}->_login( $option, $user, $password ) ) {
		return 0;
	}
	foreach my $class (@VMTurbo::use_classes) {
		$this->loadClass($class);
	}
	return 1;
}

#-----------------------------------------------------------------------------

=head2 new

Establishes a connection with the VMT server, and returns a handle to be used
in further access operations.

=over 4

=item Syntax

The first syntax type specifies the transport, ip and port as separate arguments..

 $s = new VMTurbo::Session( $transport, $ip, $port [, @tsp_args] );

 $s = VMTurbo::Session->new( $transport, $ip, $port [, @tsp_args] );

The second style of syntax uses a URL to embed all the require parameters
in a single string.

 $s = VMTurbo::Session->new( "$transport://$user:$pass\@$ip:$port" [, @tsp_args] );

 $s = VMTurbo::Session->new( "$transport://$ip:$port" [, @tsp_args] );

If no arguments are supplied, the call inspects the environment variable VMT_URL and parses
this as a URL-type specification.

 $EMN{"VMT_URL"} = "$transport://$user:$pass\@$ip:$port";
 $s = VMTurbo::Session->new( );

=item Where

=over 4

=item $transport

Is a string that indicates where transport mechanism is used
to communicate with the server. The name must be one of the recognised transport
names (see the "TRANSPORTS" section for details).

=item $ip

The IP address (or resolvable host name) of the platform on
which the server to be connected to is running.

=item $port

The port number at which the server can be reached using the specified transport. When
a URL syntax is being used, the port number can be omitted if the default port for that
transport should be used. For the three-argument sytnax, the port can be specified as
undef to have the same result.

=item $user

The name of the user for who the session must be openned.

=item $pass

The user's password.

=item @tsp_args

An optional list of transport-specific arguments.
In this version of the API - this argument is unused.

=back

=item Return

Returns a handle (perl object reference) that refers to the connection between
the client script and the server, and may be used as the context for the
other calls in this module.

=item Recommended transport

It is generally recommended to use the "Socket" or "pSocket" transport (for
optimum performance, and feature support) or "SSL"/"pSSL" if encrypted
connections are required (but note: encryption has a performance cost associated
with it).

=item Example

 $s = VMTurbo::Session new( "pSocket", "192.168.0.86", 80 );
 die "Cant open connection" unless defined $s:

 $s = VMTurbo::Session->new( 'Socket://guest:drowssap@vmtapp.org.local:8401' );

 $s = VMTurbo::Session->new( 'pSocket://guest:drowssap@vmtapp.org.local:80' );

 $s = VMTurbo::Session->new( );      ## only if env var VMT_URL is defined ##

=back

=cut

sub new {
	my $class = shift @_;
	my ( $transport, $user, $pass, $ip, $port );

	if ( @_ == 0 && defined($ENV{VMT_URL}) ) {
		$transport = $ENV{VMT_URL};
	} else {
		$transport = shift @_;
	}

	if ($transport =~ m{^([a-z]+)://(.+?):(.+?)@(.+?)(:\d+)?$}i) {
		$transport = $1;
		$user = $2;
		$pass = $3;
		$ip = $4;
		$port = $5;
	} elsif ($transport =~ m{^([a-z]+)://(.+?)(:\d+)?$}i) {
		$transport = $1;
		$user = undef;
		$pass = undef;
		$ip = $2;
		$port = $3;
	} else {
		$ip = shift @_;
		$port = shift @_;
	}
	my @tsp_args = @_;

	if (defined $port) { $port =~ s/^://; }
	my %this = ( );
	$this{connection} = VMTurbo::Connection->new( $transport, $ip, $port, @tsp_args );
	return undef unless (defined $this{connection});
	my $this = bless( \%this, $class );

	if (defined($user) && defined($pass)) {
		unless ( $this->login( $user, $pass ) ) {
			$this->close();
			return undef;
		}
	}
	return $this;
}

#------------------------------------------------------------------------------

=head2 pryClass

Gets information about a specified class.

=over 4

=item Syntax

 $info = $s->pryClass( $class_name );

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $class_name

The name of the class to be inspected.

=back

=item Return

Returns a perl reference to a hash containing information about the class. The following
hash keys are included..

=over 4

=item abstract

=item factory

=item interface

=item javaclass

=item name

=item package

=item subclasses

=item supertypes

=back

=item Example

 $info = $s->pryClass( "Mem" );
 print "Class Mem is part of package $info->{package}\n";

=back

=cut

sub pryClass {
	my ( $this, $classname ) = @_;

	return $this->_call("prycl", $classname);
}

#------------------------------------------------------------------------------

=head2 put

Modifies the values of entity attributes or changes inter-entity references.

=over 4

=item Syntax

You either call "put" in the context of the session..

 $s->put( [\$options], $entity, $name => $value [$name => $value, ...] );

Or you can use the perl hash-like syntax to set a single property..

 $entity_ref->{$name} = $value;

To use an option with the hash-like symtax, append it to the property name..

 $entity_ref->{"$name.$options"} = $value;

=item Where

=over 4

=item $options

A string of option flags, consisting of one or more of..

=over 4

=item f  (force)

Without this option being set, an attempt to set the "name" attribute to a value that is
the same as an existing entity will fail. With the option, duplicate names can be created.
Note that duplicate Uuids can NOT be set, even with this option specified.

Note: you are discouraged from creating entities with duplicate names since this can
cause a range of challenges.

=item i  (insert)

Option "i" indicates that the values specified for references are B<inserted> into the
existing set of referred-to entities. The existing references are retained. This is
only applicable to multiple-entity references. It is an error to use this option to
change the contents of a single-entity reference.

=item r  (remove)

Option "r" indicates that the values specified for references are B<removed> from the
existing set of referred-to entities. All other existing references are retained. This is
only applicable to multiple-entity references. It is an error to use this option to
change the contents of a single-entity reference.

=item s  (set)

Option "s" indicates that the values specified for references B<replace> the existing set
entirely. The existing contents of the references are replaced with the specified set.

=back

Note: Options "i", "r" and "s" are mutually exclusive and should not be specified in the same
$options string.

Note: the options "i", "r" and "s" can additionally be respecified before any of the
$name => $value pairs in the argument list. This changes the behavior for the arguments
that follow.

=item $entity

The Uuid, name or reference of the entity whose attributes and/or references are to be set.

=item $entity_ref

The perl reference for the entity to be updated (as returned by VMTurbo::Session::entity).

=item $name => $value

A set of "attribute => $value" pairs are used to set the attributes and/or references
in the entity. These attributes are set using perl's "hash" population syntax.

To unset an attribute, pass "undef" as the value.

When setting a reference, you have a number of choices (these are in addition to the use
of the "i", "r" and "s" options described above):

=over 4

=item Setting a singleton reference

If the reference is a singleton (points to just one entity) then you give the UUID or
reference of the entity as the value.

=item Emptying a singleton reference

To indicate that a singleton reference should refer to no entities, pass an empty
string or undef as the value.

=item Setting a list reference

If the reference can take multiple values then you should supply a reference to
an array that contains the UUIDs of the entities (or perl references) to be
placed in the entity reference.

=item Emptying a list reference

If you want to indicate that a list reference should contain no entities, pass
an empty array or undef as the value.

=back

=back

=item Note

The "tied hash" style of syntax can only be used to set a single attribute. To
set more than one attribute using a single message exchange, use the first
syntax show above.

=item Return

Returns 1 (meaning: true) if there are no errors.

Returns undef in the event of errors.

=item Examples

 $s->put( $mem_uuid, displayName => "Memory" );

 $mem_ref->{displayName} = "Memory";

 $s->put( $vm, ComposedOf => [ \"i", $mem1 ] );

 $s->put( \"i", $vm, ComposedOf => $mem1 );

 $s->put( $vm, ComposedOf => [ \"r", $mem2 ] );

 $s->put( $vm, ComposedOf => [ \"e", $mem1, $mem2 ] );

 $vm->{"ComposedOf.r"} = $mem1;

 Show an example of the function in use (one space indent)

=back

=cut

sub put {
	local $func = "put";
	return _put_or_create( @_ );
}

#-----------------------------------------------------------------------------

=head2 remove

Removes one or more entities (and optionally their components) from the repository.

=over 4

=item Syntax

 $n = $s->remove( [ \$options, ] $entity ... )

=item Where

=over 4

=item $s

A reference to the session to the server, established using VMTurbo::Session::new.

=item $options

=over

=item n   (non-recursive)

The function removes the entity but not its components.

=item r   (recursive)

The function removes the entity and its components.

=back

=item $entity

The Uuid, name or entity reference[s] of the object[s] to be removed.

=back

=item Return

The call returns the number of entities deleted, or undef in the event of
a failure.

=item Example

 $s->remove( \"r", "T-VM01", "T-VM02" );

=back

=cut

sub remove {
	local $func = "remove";
	my $this = shift @_;
	my $options = "";
	if (@_ > 0 && ref($_[0]) eq "SCALAR") {
		$options = ".".${shift @_};
	}
	if (@_ == 0) {
		VMTurbo::error("$func - Missing uuid argument[s]");
		return 0;
	}
	return $this->_call("rem$options", @_);
}

#------------------------------------------------------------------------------

=head2 setXMLParser

Allows the script to control how XML return data is handled by the API. By default
the "XML::Simple" module is used to convert XML data to a perl hash.

=over 4

=item Syntax

 $s->setXMLParser( );               -- to restore default XML parsing logic
 $s->setXMLParser( undef );         -- to turn XML parsing off
 $s->setXMLParser( \&function );    -- to select custom XML parsing logic function

=item Where

=over 4

=item $s

The handle of the client/server session who's XML parsing logic is to be modified.

=item &function

The name of an existing perl subroutine that is used to parse an XML document string. The
function template should look like this..

 sub function {
         my ( $xml_string ) = @_;
	 ... do the work here ...
	 return $result;
 }

The function can return any valid perl data type. It is generally recommended that the
return is a reference to a perl hash - but other use cases are allowed.

If an "undef" function argument is specifed (2nd syntax, above) then no XML parsing is
performed, and XML returns are passed to the client script as unparsed strings.

If no function argument is specified (1st syntax, above) then the default XML parsing logic
(using XML::Simple) is selected.

=back

=item Return

"true" means that the call was successful, and "false" means it failed. Note that "true"
does NOT imply that the logic contained in the customer XML parser function is correct;
mearly that the API has successfully registered the logic for use in XML parsing.

=item Example

 $xs = XML::Simple->new( );
 $s->setXMLParser( sub { return $xs->parse_string( $_[0] ); } );

=back

=cut

sub setXMLParser {
	local $func = "setXMLParser";
	my $this = shift @_;
	if (@_ == 0) {
		delete $this->{XMLParser};
		return true;
	} elsif (@_ == 1 && !defined($_[0])) {
		$this->{XMLParser} = undef;
		return true;
	} elsif (@_ == 1 && ref($_[0]) eq "CODE") {
		$this->{XMLParser} = $_[0];
		return true;
	}
	VMTurbo::error( "$func - invalid arguments" );
	return false;
}

#=============================================================================
# Supporting utility functions
#=============================================================================

sub _call {
	my ( $this, @args ) = @_;

	foreach (@args) {
		if (ref($_) =~ m/^VMTurbo::Entity::/) {
			$_ = $_->{-uuid};
		}
	}
	my $cmd = shift @args;
	$cmd =~ s/\.$//;
	if ($this->{_batch_flag}) {
		return $this->{connection}->_format_request( $cmd, @args );
	}
	my $rtn = $this->{connection}->_call( $cmd, @args );
	return undef unless defined $rtn;
	return _render( $this, $rtn );
}


sub _render {
	my ( $this, $rtn ) = @_;

	my $state = $rtn->{state};
	my $type = $rtn->{type};
	my $encoding = $rtn->{encoding};
	my $mesg = $rtn->{mesg};

	if ($state eq "ERR") {
		VMTurbo::error("$func - $mesg");
		return undef;
	}

	if ($type =~ m/^[socuida]$/) {
		return _decode($encoding, $mesg);

	} elsif ($type eq "l") {
		if ($VMTurbo::n_bits < 64) {
			return Math::BigFloat->new(_decode($encoding, $mesg));
		} else {
			return _decode($encoding, $mesg);
		}

	} elsif ($type eq "v") {
		my $rtn = _decode($encoding, $mesg);
		return $rtn eq "" ? 1 : $rtn;

	} elsif ($type eq "e") {
		my $obj = _decode($encoding, $mesg);
		return undef unless defined $obj;
		return $this->entity( $obj->[0], $obj->[1] );

	} elsif ($type eq "xml" && $encoding eq "raw") {
		my $decoder = \&VMTurbo::parseXML;
		if (exists $this->{XMLParser}) {
			$decoder = $this->{XMLParser};
		}
		return (defined $decoder) ? &{$decoder}( $mesg ) : $mesg;

	} elsif ($type eq "null") {
		return undef;

	} elsif ($type eq "b") {
		return (_decode($encoding, $mesg) =~ m/^t/i) ? 1 : 0;

	} elsif ($type =~ m/^list<[socuid]>$/) {
		my @rtn = map { _decode($encoding, $_); } split(/\r?\n/, $mesg, -1);
		return \@rtn;

	} elsif ($type eq "list<e>") {
		my @rtn = map {
			my ($c,$u) = @{_decode($encoding, $_)};
			$this->entity( $c, $u )
		} split(/\r?\n/, $mesg, -1);
		return \@rtn;

	} elsif ($type =~ m/^map<[socuid],[socuid]>$/ && $encoding eq "kv") {
		my %hash = ( );
		foreach my $rec ( split(/\r?\n/, $mesg, -1) ) {
			my ( $k, $v ) = split(/=/, $rec, 2);
			$hash{_URIdecode($k)} = _URIdecode($v);
		}
		return \%hash;

	} elsif ($type eq "list<*>") {
		my @rtnarray = ( );

		while ( $mesg ne "" ) {
			my ( $info, $remainder ) = split( /\n/, $mesg, 2);

			my ( $type, $encoding, $clen, $plen, $none ) = split(/ /, $info);
			if (!defined($plen) || defined($none)) {
				VMTurbo::error("$func - Unexpected returned data format");
				return undef;
			}
			my $state = ($type =~ m/^\+/) ? "OK" : "ERR";
			$type = substr($type, 1);
			my $value = substr($remainder, 0, $clen);
			my $rtn = _render( $this, { state=>$state, type=>$type, encoding=>$encoding, mesg=>$value } );
			push @rtnarray, $rtn;
			$mesg = substr($remainder, $clen + $plen);
		}
		return \@rtnarray;

	} elsif ($type eq "map<s,*>") {
		my %rtnhash = ( );

		while ( $mesg ne "" ) {
			my ( $name, $info, $remainder ) = split(/\n/, $mesg, 3);
			unless (defined $remainder) {
				VMTurbo::error("$func - Unexpected returned data format");
				return undef;
			}

			my ( $type, $encoding, $clen, $plen, $none ) = split(/ /, $info);
			if (!defined($plen) || defined($none)) {
				VMTurbo::error("$func - Unexpected returned data format");
				return undef;
			}

			my $state = ($type =~ m/^\+/) ? "OK" : "ERR";
			$type = substr($type, 1);
			my $value = substr($remainder, 0, $clen);
			my $rtn = _render( $this, { state=>$state, type=>$type, encoding=>$encoding, mesg=>$value } );
			$rtnhash{$name} = $rtn;
			$mesg = substr($remainder, $clen + $plen);
		}
		return \%rtnhash;
	}
	VMTurbo::error("$func - Unknown return type: $type");
	return undef;
}


sub _decode {
	my ($method, $data) = @_;
	if ($method eq "raw") { return $data; }
	if ($method eq "line") { return _URIdecode($data); }
	if ($method eq "bsv") { return [ map { _URIdecode($_); } split( /\|/, $data, -1) ];
	}
	VMTurbo::error("$func - Unknown encoding: $method" );
	return undef;
}


sub _URIdecode {
	my ( $str ) = @_;
	$str =~ s/%([0-9a-f][0-9a-f])/chr(hex($1))/gei;
	return $str;
}

# The "put" and "create" commands are so similar in the way they handle arguments etc that it
# makes sense to use shared code...
sub _put_or_create {
	my $this = shift @_;
	my $options = "";
	if (ref($_[0]) eq "SCALAR") {
		$options = "." . ${shift @_};
	}
	my $class_or_uuid = shift @_;
	if (@_ % 2 != 0) {
		VMTurbo::error("$func - Invalid number/type of parameters");
		return undef;
	}

	if (ref($class_or_uuid) =~ m/^VMTurbo::Entity::/) {
		if ($class_or_uuid->{-singleton}) {
			$class_or_uuid = $class_or_uuid->{-class};
			if ($options !~ m/c/) { $options = ( $options eq "" ? ".c" : "${options}c" ); }
		} else {
			$class_or_uuid = $class_or_uuid->{-uuid};
		}
	}

	my $default_op = "=";
	if ($options =~ m/s/) { $default_op = "="; $options =~ s/s//g; }
	if ($options =~ m/i/) { $default_op = "+="; $options =~ s/i//g; }
	if ($options =~ m/r/) { $default_op = "-="; $options =~ s/r//g; }

	my @cmd = ( substr($func, 0, 3).$options, $class_or_uuid );
	while (@_) {
		my $key = shift @_;
		if (ref($key) eq "SCALAR") {
			if ($$key =~ m/s/) { $default_op = "="; }
			if ($$key =~ m/i/) { $default_op = "+="; }
			if ($$key =~ m/r/) { $default_op = "-="; }
			next;
		}
		my $value = shift @_;
		my $op = $default_op;
		if (!defined $value) {
			$value = "";
			$op = "#=";
		} elsif (ref($value) eq "ARRAY") {
			my @values = @{$value};
			if (@values > 0 && ref($values[0]) eq "SCALAR") {
				if ( ${$values[0]} eq "i") {
					shift @values;
					$op = "+=";
				} elsif (${$values[0]} eq "r") {
					shift @values;
					$op = "-=";
				} else {
					VMTurbo::error("$func - Invalid option in reference list");
					return undef;
				}
			}
			foreach (@values) {
				if (ref($_) =~ m/^VMTurbo::Entity::/) {
					$_ = $_->_getUuid();
				}
#				$_ = uri_escape($_);
			}
			$value = join("|", @values);
		} elsif (ref($value) =~ m/^VMTurbo::Entity::/) {
# TODO: The handling of singleton objects may be broken because the UUID will be
#       a class name.
#       Also: need to uri_escape the uuid here to make it properly parseable by the
#       java code - in the same way as we would if it was a list of uuids.
#       And: How to do we handle objects specified by uuid or name strings rather than
#       entity references? How do know whether to encode, or not? For now we live with
#       non-encoding in the hope that there are no "%" in the uuid or name.
			$value = $value->_getUuid();
#		} else {
#			$value = uri_escape( $value );
		}
		push @cmd, "$key$op$value";
	}
	return $this->_call(@cmd);
}


1;
