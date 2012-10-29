package Protocol::Tarantool;

use 5.008008;
use strict;
use warnings;

use base qw(Exporter);


our %EXPORT_TAGS = (
    client      => [ qw( ) ],
    constant    => [
        qw(
            TNT_INSERT TNT_SELECT TNT_UPDATE TNT_DELETE TNT_CALL TNT_PING
            TNT_FLAG_RETURN TNT_FLAG_ADD TNT_FLAG_REPLACE TNT_FLAG_BOX_QUIET
            TNT_FLAG_NOT_STORE
        )
    ],
);

our @EXPORT_OK = ( map { @$_ } values %EXPORT_TAGS );
$EXPORT_TAGS{all} = \@EXPORT_OK;
our @EXPORT = @{ $EXPORT_TAGS{client} };


our $VERSION = '0.01';

require XSLoader;
XSLoader::load('Protocol::Tarantool', $VERSION);

sub select_pp {
	my ($reqid, $ns, $idx, $off, $lim, $keys, $fmt, $df) = @_;
	return pack(
		'V8 (a*)*',
		TNT_SELECT(), 0, $reqid,
		$ns, $idx, $off, $lim,
		0+@$keys,
		
		
		map{ pack('V w/a*', 0+@$_, @$_) } @$keys
	);
}

# Preloaded methods go here.

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

Protocol::Tarantool - Perl extension for blah blah blah

=head1 SYNOPSIS

  use Protocol::Tarantool;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for Protocol::Tarantool, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

A. U. Thor, E<lt>mons@(none)E<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2012 by A. U. Thor

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.16.1 or,
at your option, any later version of Perl 5 you may have available.


=cut
