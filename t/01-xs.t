#!/usr/bin/perl

{
	package # hide
		MyBuilder;
	
	sub _print {
		$_[1] =~ s{^(.+?)(#.+?|)(?:\n|\z)}{\e[$_[0]{__color};1m$1\e[35m$2\e[0m}s;
		goto &{ $_[0]->can('SUPER::_print') };
	}
	
	sub ok {
		$_[0]{__color} = $_[1] ? 32 : $_[0]->in_todo ? 33 : 31;
		goto &{ $_[0]->can('SUPER::ok') };
	}
}

use warnings;
use strict;
use utf8;
use open qw(:std :utf8);
use lib qw(lib ../lib);
use lib qw(blib/lib blib/arch ../blib/lib ../blib/arch);

use t::mytest tests    => 157;
use Encode qw(decode encode);

BEGIN {
	if (-t STDOUT) {
	my $b = Test::More->builder;
	@MyBuilder::ISA = ( ref $b );
	bless $b, 'MyBuilder';
	}
}

=for rem
pass 'x1';
fail 'x2';
{
	local $TODO = "Testing";
	pass 'x3';
	fail 'x4';
}
__END__
=cut

BEGIN {
    my $builder = Test::More->builder;
    binmode $builder->output,         ":utf8";
    binmode $builder->failure_output, ":utf8";
    binmode $builder->todo_output,    ":utf8";

    use_ok 'Protocol::Tarantool', ':constant';
    use_ok 'File::Spec::Functions', 'catfile';
    use_ok 'File::Basename', 'dirname';
}

like TNT_INSERT,            qr{^\d+$}, 'TNT_INSERT';
like TNT_SELECT,            qr{^\d+$}, 'TNT_SELECT';
like TNT_UPDATE,            qr{^\d+$}, 'TNT_UPDATE';
like TNT_DELETE,            qr{^\d+$}, 'TNT_DELETE';
like TNT_CALL,              qr{^\d+$}, 'TNT_CALL';
like TNT_PING,              qr{^\d+$}, 'TNT_PING';

like TNT_FLAG_RETURN,       qr{^\d+$}, 'TNT_FLAG_RETURN';
like TNT_FLAG_ADD,          qr{^\d+$}, 'TNT_FLAG_ADD';
like TNT_FLAG_REPLACE,      qr{^\d+$}, 'TNT_FLAG_REPLACE';
like TNT_FLAG_BOX_QUIET,    qr{^\d+$}, 'TNT_FLAG_BOX_QUIET';
like TNT_FLAG_NOT_STORE,    qr{^\d+$}, 'TNT_FLAG_NOT_STORE';

my $LE = $] > 5.01 ? '<' : '';


# SELECT
diag "Testing select";
my $sbody = Protocol::Tarantool::select( 9, 8, 7, 6, 5, [ [4], [3] ] );
ok defined $sbody, '* select body';

my @a = unpack "( L$LE )*", $sbody;
is $a[0], TNT_SELECT, 'select type';
is $a[1], length($sbody) - 3 * 4, 'select - body length';
is $a[2], 9, 'select - request id';
is $a[3], 8, 'select - space no';
is $a[4], 7, 'select - index no';
is $a[5], 6, 'select - offset';
is $a[6], 5, 'select - limit';
is $a[7], 2, 'select - tuple count';
ok !eval { Protocol::Tarantool::select( 1, 2, 3, 4, 5, [ 6 ] ) }, 'select - keys format';
like $@ => qr{ARRAYREF of ARRAYREF}, 'select - error string';

# PING
diag "Testing ping";
$sbody = Protocol::Tarantool::ping( 11 );
ok defined $sbody, '* ping body';
@a = unpack "( L$LE )*", $sbody;
is $a[0], TNT_PING, 'ping type';
is $a[1], length($sbody) - 3 * 4, 'ping - body length';
is $a[2], 11, 'ping - request id';


# insert
diag "Testing insert";
$sbody = Protocol::Tarantool::insert( 12, 13, 14, [ 'a', 'b', 'c', 'd' ]);
ok defined $sbody, '* insert body';
@a = unpack "( L$LE )*", $sbody;
is $a[0], TNT_INSERT, 'insert type';
is $a[1], length($sbody) - 3 * 4, 'body length';
is $a[2], 12, 'request id';
is $a[3], 13, 'space no';
is $a[4], 14, 'flags';
is $a[5], 4,  'tuple size';
#diag xd $sbody;
#__END__

# delete
$sbody = Protocol::Tarantool::delete( 119, 120, 121, [ 122, 123 ] );
ok defined $sbody, '* delete body';
@a = unpack "( L$LE )*", $sbody;
is $a[0], TNT_DELETE, 'delete type';
is $a[1], length($sbody) - 3 * 4, 'body length';
is $a[2], 119, 'request id';

is $a[3], 120, 'space no';

if (TNT_DELETE == 20) {
    ok 1, '# skipped old delete code';
    is $a[4], 2,  'tuple size';
} else {
    is $a[4], 121, 'flags';  # libtarantool ignores flags
    is $a[5], 2,  'tuple size';
}

# call
$sbody = Protocol::Tarantool::lua( 124, 125, 'tproc', [ 126, 127 ]);
ok defined $sbody, '* call body';
@a = unpack "L$LE L$LE L$LE L$LE w/Z* L$LE L$LE", $sbody;
is $a[0], TNT_CALL, 'call type';
is $a[1], length($sbody) - 3 * 4, 'body length';
is $a[2], 124, 'request id';
is $a[3], 125, 'flags';
is $a[4], 'tproc',  'proc name';
is $a[5], 2, 'tuple size';

# update
diag "Testing update";
my @ops = map { [ int rand 100, $_, int rand 100, 'I' ] }
    qw(+ & | ^ = !),'#';
push @ops, [ int(rand 100), ':', int(rand 100), int(rand 100), "somestring"  ];

$sbody = Protocol::Tarantool::update( 15, 16, 17, [ 18 ], \@ops);
ok defined $sbody, '* update body';
@a = unpack "( L$LE )*", $sbody;
is $a[0], TNT_UPDATE, 'update type';
is $a[1], length($sbody) - 3 * 4, 'update - body length';
is $a[2], 15, 'update - request id';
is $a[3], 16, 'update - space no';
is $a[4], 17, 'update - flags';
is $a[5], 1,  'update - tuple size';

my %pk; my $raw = $sbody;
( @pk{ qw(type len id) }, $raw) = unpack 'V3 a*', $raw;
if ($pk{type} == TNT_UPDATE) {
	( @pk{ qw( space flags tuple ) }, $raw) = unpack 'V2 V a*', $raw;
	#@{ $pk->{tuple} = [  ]
}

diag "Testing lua";
$sbody = Protocol::Tarantool::lua( 124, 125, 'tproc', [  ]);

# parser
{
local $SIG{__WARN__} = sub {};
ok !eval { Protocol::Tarantool::response( undef ) }, '* parser: undef';
}
my $res = Protocol::Tarantool::response( '' );
diag explain $res;
isa_ok $res => 'HASH', 'empty input';
like $res->{errstr}, qr{too short}, 'error message';
is $res->{status}, 'buffer', 'status';

my $data;
for (TNT_INSERT, TNT_UPDATE, TNT_SELECT, TNT_DELETE, TNT_CALL, TNT_PING) {
    my $msg = "test message";
    $data = pack "L$LE L$LE L$LE L$LE Z*",
        $_, 5 + length $msg, $_ + 100, 0x0101, $msg;
    my $len = Protocol::Tarantool::peek_size( \$data );
    is $len, length($msg) + 5, 'length ok';
    $res = Protocol::Tarantool::response( $data );
    #diag explain $res;
    isa_ok $res => 'HASH', 'well input ' . $_;
    is $res->{id}, $_ + 100, 'request id';
    is $res->{type}, $_, 'request type';

    unless($res->{type} == TNT_PING) {
        is $res->{status}, 'error', "status $_";
        is $res->{code}, 0x101, 'code';
        is $res->{errstr}, $msg, 'errstr';
    }
}

my $cfg_dir = catfile dirname(__FILE__), 'test-data';
ok -d $cfg_dir, 'directory with test data';
my @bins = glob catfile $cfg_dir, '*.bin';

for my $bin (@bins) {
    my ($type, $err, $status) =
        $bin =~ /(?>0*)?(\d+?)-0*(\d+)-(\w+)\.bin$/;
    next unless defined $bin;
    next unless $type;
    ok -r $bin, "$bin is readable";

    ok open(my $fh, '<:raw', $bin), "open $bin";
    my $pkt;
    { local $/; $pkt = <$fh>; }
    ok $pkt, "response body was read from $bin";
    #diag xd $pkt;

    my $res = Protocol::Tarantool::response( $pkt );
    #diag explain $res;
    SKIP: {
        skip 'legacy delete packet', 4 if $type == 20 and TNT_DELETE != 20;
        my $ok = 1;
        $ok &&= is $res->{status}, $status, $bin.' - status';
        $ok &&= is $res->{type}, $type, $bin.' - type';
        $ok &&= is $res->{code}, $err, $bin.' - error code';
        $ok &&= ok ( !($res->{code} xor $res->{errstr}), 'errstr' );
        $ok or diag explain $res;
    }
}

