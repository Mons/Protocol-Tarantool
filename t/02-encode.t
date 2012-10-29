#!/usr/bin/perl

use lib '..';
use t::mytest tests => 108;
use Protocol::Tarantool ();
use uni::perl ':xd';

use String::Diff;

sub bcmp {
	my ($x,$y) = @_;
	my $hx = unpack 'H*', $x;
	my $hy = unpack 'H*', $y;
	my $diff = String::Diff::diff(
		$hx,$hy,
		remove_open  => "\e[31;1m",
		remove_close => "\e[0m",
		append_open  => "\e[32;1m",
		append_close => "\e[0m",
		
	);
	for ($diff->[0], $diff->[1]) {
	my @ax = 
		m{
			\G(
					(?:\e\[.+?m|)
				[\da-f]
					(?:\e\[.+?m|)
				[\da-f]
					(?:\e\[.+?m|)
			)
		}gx;
	diag explain $_;
	#diag explain \@ax;
	for my $r (0..1+@ax/16) {
		printf "# [%04x]  ",$r * 16;
		for my $c (0..15) {
			print " " if $c % 4 == 0;
			printf "%02s ", $ax[ $r*16 + $c ];
		}
		print "   ";
		for my $c (0..15) {}
		print "\n";
	}
	print "\n";
	}
}


{
	my %uniq;
	my @tests = sort map { $_->[0] } grep { !$uniq{$_->[1]}++ } map {
		m{^(.+(\d+))[^/]+$}; [$1, $2]
	} <tests/select/*>, <t/tests/select/*>;
	
	for (@tests) {
		my ($num) = m{(\w+/\d+)[^/]*?$};
		my %s = %{ do( "$_.source" ) };
		my %r = %{ do( "$_.result" ) };
		my $bin = do { open my $f,'<:raw', "$_.binary"; local $/; <$f>; };
		
		my $pkt = Protocol::Tarantool::select( $s{id}, $s{ns}, $s{idx}, $s{offset}, $s{limit}, $s{keys}, $s{format} ? $s{format} : () );
		ok !utf8::is_utf8($pkt), "$num: not utf8";
		ok $pkt eq $bin, "$num: binary eq"
			or do{
				bcmp( $bin,$pkt );
				diag("Should: ");
				packet_dump( $bin );
				diag("Have: ");
				packet_dump( $pkt );
			};
		
		#packet_dump( $pkt );
		is_deeply parse_packet($pkt), \%r, "$num: struct";
	}
}

{
	my %uniq;
	my @tests = sort map { $_->[0] } grep { !$uniq{$_->[1]}++ } map {
		m{^(.+(\d+))[^/]+$}; [$1, $2]
	} <tests/delete/*>, <t/tests/delete/*>;
	
	for (@tests) {
		my ($num) = m{(\w+/\d+)[^/]*?$};
		my %s = %{ do( "$_.source" ) };
		my %r = %{ do( "$_.result" ) };
		my $bin = do { open my $f,'<:raw', "$_.binary"; local $/; <$f>; };
		
		my $pkt = Protocol::Tarantool::delete( $s{id}, $s{ns}, $s{flags}, $s{key}, $s{format} ? $s{format} : () );
		ok !utf8::is_utf8($pkt), "$num: not utf8";
		ok $pkt eq $bin, "$num: binary eq"
			or diag( xd $bin ), diag(xd $pkt);
		my $p = parse_packet($pkt);
		is_deeply $p, \%r, "$num: struct" or diag explain $p;
	}
}

{
	my %uniq;
	my @tests = sort map { $_->[0] } grep { !$uniq{$_->[1]}++ } map {
		m{^(.+(\d+))[^/]+$}; [$1, $2]
	} <tests/insert/*>, <t/tests/insert/*>;
	
	for (@tests) {
		my ($num) = m{(\w+/\d+)[^/]*?$};
		my %s = %{ do( "$_.source" ) };
		my %r = %{ do( "$_.result" ) };
		my $bin = do { open my $f,'<:raw', "$_.binary"; local $/; <$f>; };
		
		my $pkt = Protocol::Tarantool::insert( $s{id}, $s{ns}, $s{flags}, $s{key}, $s{format} ? $s{format} : () );
		ok !utf8::is_utf8($pkt), "$num: not utf8";
		ok $pkt eq $bin, "$num: binary eq"
			or do{
				bcmp( $bin,$pkt );
				diag("Should: ");
				packet_dump( $bin );
				diag("Have: ");
				packet_dump( $pkt );
			};
		my $p = parse_packet($pkt);
		is_deeply $p, \%r, "$num: struct" or diag explain $p;
	}
}

{
	my %uniq;
	my @tests = sort map { $_->[0] } grep { !$uniq{$_->[1]}++ } map {
		m{^(.+(\d+))[^/]+$}; [$1, $2]
	} <tests/lua/*>, <t/tests/lua/*>;
	
	for (@tests) {
		my ($num) = m{(\w+/\d+)[^/]*?$};
		my %s = %{ do( "$_.source" ) };
		my %r = %{ do( "$_.result" ) };
		my $bin = do { open my $f,'<:raw', "$_.binary"; local $/; <$f>; };
		
		my $pkt = Protocol::Tarantool::lua( $s{id}, $s{flags}, $s{proc}, $s{args}, $s{format} ? $s{format} : () );
		ok !utf8::is_utf8($pkt), "$num: not utf8";
		ok $pkt eq $bin, "$num: binary eq"
			or diag( xd $bin ), diag(xd $pkt);
		my $p = parse_packet($pkt);
		is_deeply $p, \%r, "$num: struct" or diag explain $p;
	}
}


{
	my %uniq;
	my @tests = sort map { $_->[0] } grep { !$uniq{$_->[1]}++ } map {
		m{^(.+(\d+))[^/]+$}; [$1, $2]
	} <tests/update/*>, <t/tests/update/*>;
	
	for (@tests) {
		my ($num) = m{(\w+/\d+)[^/]*?$};
		my %s = %{ do( "$_.source" ) };
		my %r = %{ do( "$_.result" ) };
		my $bin = do { open my $f,'<:raw', "$_.binary"; local $/; <$f>; };
		
		my $pkt = Protocol::Tarantool::update( $s{id}, $s{ns}, $s{flags}, $s{key}, $s{ops}, $s{format} ? $s{format} : () );
		ok !utf8::is_utf8($pkt), "$num: not utf8";
		ok $pkt eq $bin, "$num: binary eq"
			or do{
				diag( xd $bin ), diag(" "), diag(xd $pkt), diag(" ");
				bcmp( $bin,$pkt );
				packet_dump( $bin );
				packet_dump( $pkt );
				exit;
			};
		#packet_dump( $pkt );
		my $p = parse_packet($pkt);
		is_deeply $p, \%r, "$num: struct" or diag explain $p;
	}
}



__END__
for (1, 0xff, 0xffffff, 0x7fffffff) {
	w($_);
	say render parse_packet(DR::Tarantool::_pkt_select( w, w, w, w, w, [ [w], [w] ] ));
}

{
        count => 2,
        id => 3,
        index => 5,
        len => 32,
        limit => 7,
        offset => 6,
        space => 4,
        trash => "",
        type => 17,
        tuples => [
                ["8:s"],
                ["9:s"],
        ],
}

{
        count => 2,
        id => 257,
        index => 259,
        len => 36,
        limit => 261,
        offset => 260,
        space => 258,
        trash => "",
        type => 17,
        tuples => [
                ["262:s"],
                ["263:s"],
        ],
}

{
        count => 2,
        id => 16777217,
        index => 16777219,
        len => 46,
        limit => 16777221,
        offset => 16777220,
        space => 16777218,
        trash => "",
        type => 17,
        tuples => [
                ["16777222:s"],
                ["16777223:s"],
        ],
}

{
        count => 2,
        id => 2147483649,
        index => 2147483651,
        len => 50,
        limit => 2147483653,
        offset => 2147483652,
        space => 2147483650,
        trash => "",
        type => 17,
        tuples => [
                ["2147483654:s"],
                ["2147483655:s"],
        ],
}
