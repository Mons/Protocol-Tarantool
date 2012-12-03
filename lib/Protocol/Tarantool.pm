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


our $VERSION = '0.02';

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

Protocol::Tarantool - Binary protocol of Tarantool/Box database

=head1 SYNOPSIS

    use Protocol::Tarantool;

    my $key1 = [ 'value1','value2' ];
    my $key2 = [ 'value3','value4' ];
    my $tuple = [ 1,2'data',3,'moredata'];

    # Pack request packets
    my $packet = Protocol::Tarantool::select( $req_id, $sp_no, $idx_no, $offset, $limit, [ $key1, $key2 ]);
    my $packet = Protocol::Tarantool::insert( $req_id, $sm_no, $flags, $tuple);
    my $packet = Protocol::Tarantool::delete( $req_id, $sm_no, $flags, $tuple);
    my $packet = Protocol::Tarantool::lua(    $req_id, $flags, $function_name, [ $arg1, $arg2, $arg3 ]);

    # Unpack response packet
    # detect total size of packet by first bytes of header (need first 8 bytes). Return -1 if not enough data
    my $size_of_packet = Protocol::Tarantool::peek_size( \$packet );

    my $response = Protocol::Tarantool::response( $packet );

=head2 EXPORT

None by default.

=head1 SEE ALSO

=over 4

=item L<AnyEvent::Tarantool> - AnyEvent client that uses this protocol

=item L<DR::Tarantool> - Another sync and async implementation

=item L<MR::Tarantool::Box> - First client by Mail.Ru

=back

=head1 AUTHOR

Mons Anderson <mons@cpan.org>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

=head1 COPYRIGHT

Copyright 2012 Mons Anderson, Mail.ru, all rights reserved.

=cut
