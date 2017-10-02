package FileUpload;

use strict;
use IO::Handle;
use IO::File;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

require Exporter;
require AutoLoader;

@ISA = qw(Exporter AutoLoader);
@EXPORT = qw();
@EXPORT_OK = qw();

$VERSION = '0.07';

sub new {
  my($pkg) = shift;
  my($self) = {'_ufile' => shift};
  my($fh) = new IO::Handle->fdopen(fileno($self->{'_ufile'}), "r") ||
	return undef;

  $self->{'_fh'} = $fh;

  $self->{'_deny_ext'} = [];

  $self->{'_allow_char'} = ['\w', '\-', '\.'];

  bless $self, $pkg;
}

sub _allow_char () { $_[0]->{'_allow_char'} }

sub _deny_ext () { $_[0]->{'_deny_ext'} }

sub allow_char (;@) {
  return @{$_[0]->_allow_char} unless @_ > 1;
  my($self) = shift;
  push(@{$self->_allow_char}, @_);
  $self->allow_char;
}

sub deny_ext (;@) {
  return @{$_[0]->_deny_ext} unless @_ > 1;
  my($self) = shift;
  map { my($ext) = $_; $ext =~ s/^\.//g; push(@{$self->_deny_ext}, $ext) } @_;
  $self->deny_ext;
}

sub fh () { $_[0]->{'_fh'} }

sub get_line () { $_[0]->fh->getline }

sub get_lines () { ($_[0]->fh->getlines) }

sub get_pos () { tell($_[0]->fh) }

sub is_filename_allowed ($) {
  my($chars) = join("", @{$_[0]->_allow_char});
  return ($_[1] =~ /^[$chars]+$/) ? 1 : 0;
}

sub is_ext_denied ($) {
  my($exts) = join("|", @{$_[0]->_deny_ext});
  return 1 if $_[1] =~ /\.($exts)$/i;
  return 0;
}

sub orig_filename () { 
  my($path) = $_[0]->orig_filepath;
  $path =~ s/^.*[\\\/]([^\\\/]+)$/$1/g; 
  return scalar($path);
}

sub orig_filepath () { scalar($_[0]->ufile) }

sub save () { $_[0]->save_as($_[0]->orig_filename) }

sub save_as ($) {
  my($self) = shift;
  my($newfilename) = shift;
  my($currpos, $newfh, $buf, $total, $n, $n2);

  $total = $n = $n2 = 0;
  $buf = undef;
  $newfh = undef;
  $currpos = $self->get_pos;
  $self->set_pos(0);

  ($! = 2, return 0) unless $newfilename;

  ($! = 22, return 0) if ($self->is_ext_denied($newfilename));
  ($! = 22, return 0) unless $self->is_filename_allowed($newfilename);

  $newfh = new IO::File $newfilename, "w";
  return 0 unless $newfh;
  if ($^O =~ /Win32/) {
    binmode($newfh);
    binmode($self->fh);
  }
  while (($n = read($self->fh, $buf, 1024))) { 
    $n2 = syswrite($newfh, $buf, $n);
    $total += $n2;
  }
  $newfh->close;
  $self->set_pos($currpos);

  return $total;
}

sub set_pos ($) { seek($_[0]->fh, $_[1], 0) }

sub size () { scalar((stat($_[0]->fh))[7]) }

sub ufile () { $_[0]->{'_ufile'} }

1;
__END__

=head1 NAME

FileUpload - Module for HTTP File Uploading.

=head1 SYNOPSIS

  use strict;
  use FileUpload;
  use CGI qw(-private_tempfiles :standard);
  use vars qw($q $fu);

  $q = new CGI;
  $fu = new FileUpload($cgi->param('ufile'));

  # The uploaded file's size.
  $size = $fu->size;

  # Get the filename.
  $filename = $fu->orig_filename;

  # Get the full path of the file.
  # eg result: C:\test.html
  $filepath = $fu->orig_filepath;

  # Get the characters allowed.
  @chararr = $fu->allow_char;

  # Add the characters allowed.
  @chararr = $fu->allow_char('char1', 'char2');

  # Get the extensions not allowed.
  @extarr = $fu->deny_ext;

  # Add the extensions not allowed.
  @extarr = $fu->deny_ext('cgi', 'shtml');

  # Save the uploaded file as the original filename.
  $byteswritten = $fu->save;

  # Save the uploaded file by using "newfilename".
  $byteswritten = $fu->save_as("newfilename");

  # Get the filehandle's current position.
  $pos = $fu->get_pos;

  # Set the filehandle position.
  $fu->set_pos($newpos);

  # Read a line.
  $aline = $fu->get_line;

  # Read many lines.
  @lines = $fu->get_lines;

=head1 DESCRIPTION

This module helps you to process uploaded file easier.

=over 4

=item new UPLOADEDFILE

UPLOADEDFILE is the value returned by CGI->param() for the uploaded file.

=item allow_char CHAR1, [CHAR2...]

CHAR1, [CHAR2...] are the characters that allowed for the saved filename.
Defaults are '\w', '\-' and '\.'.
This is using regular expression's pattern match, so please check "perldoc perlre"
for more details of patterns defined.
Returns the array of the characters allowed.

Alert: We should set backslash and slash with single quote, allow_char('\\\/').

=item deny_ext EXT1, [EXT2...]

EXT1, [EXT2...] are the extensions that the saved filename can
not end with. Returns the array of the extensions not allowed.

=item get_line

Read a line from the uploaded file.

=item get_lines

Read multiple lines from the uploaded file.
Returns an array of strings.

=item get_pos

Returns the current position for uploaded file's FILEHANDLE.

=item orig_filename

Returns the original filename string.

=item orig_filepath

Returns the filepath string, eg result: C:\test.html.

=item save

Save the uploaded file as the original filename.
Returns the number of bytes actually wrote, or undef if there was an error.

=item save_as NEWFILEPATH

Save the uploaded file to the NEWFILEPATH.
Returns the number of bytes actually wrote, or undef if there was an error.

=item set_pos POSITION

Set the current position of filehandle to POSITION.
Returns 1 upon success, 0 otherwise.

=item size

Returns the size of the uploaded file upon success, undef otherwise.

=head1 NOTICES

=item modules

Make sure all the related modules are installed properly.
Please refer to the README file in the compressed file.

=item enctype

In the form tag, we must specify "enctype=multipart/form-data"
to have HTTP File Upload supported, else your browser will not
attach any selected file in the submission of form data.

=item permission

CGI script owner must have enough write privilege to save the
uploaded file into the specified directory.

=item allow_char

For security reason, by default, FileUpload module only allows
alphanumeric, dash and dot in filename. You might need to
call allow_char function to allow characters like "/" or "\"
if your file going to be saved outsides the current directory. 

=item deny_ext

You might want to deny some file extensions to avoid users
upload some virus/trojan files with this function.

=back 

=head1 CHANGES

0.01 -> 0.02    orig_filename() returns a wrong filename.

0.02 -> 0.03	save_as() use read() now instead of sysread() to solve
some reading errors.

0.03 -> 0.04	Call the binmode() when we read and write for files in
Win32 platforms.

0.04 -> 0.05	Checking of $newfh should be done after it's being 
assigned, should not be after the OS checking's if block.

0.05 -> 0.06	Makefile.PL will check for IO module which is required.
And new() will return undef if an error occurred.

0.06 -> 0.07	Some NOTICES are added to help the developers.

=head1 DOWNLOAD

http://www.tneoh.zoneit.com/perl/FileUpload/download/

=head1 AUTHOR


Simon Tneoh Chee-Boon   tneohcb@pc.jaring.my

Copyright (c) 2000-2002 Simon Tneoh Chee-Boon. All rights reserved.
This program is free software; you can redistribute it and/or
modify it under the same terms as Perl itself.

=head1 SEE ALSO

CGI, IO::Handle and IO::File

=cut
