package #hide
	t::mytest::builder;

	sub _print {
		$_[1] =~ s{^(.+?)(#.+?|)(?:\n|\z)}{\e[$_[0]{__color};1m$1\e[35m$2\e[0m}s;
		goto &{ $_[0]->can('SUPER::_print') };
	}
	
	sub ok {
		$_[0]{__color} = $_[1] ? 32 : $_[0]->in_todo ? 33 : 31;
		goto &{ $_[0]->can('SUPER::ok') };
	}

package #hide
	t::mytest;

use strict;
use warnings;
use Test::More ();

sub import {
	strict->import;
	warnings->import;
	my $b = Test::More->builder;
	if (-t STDOUT) {
		warn "Using color";
		@t::mytest::builder::ISA = ( ref $b );
		bless $b, 't::mytest::builder';
	}
	binmode $b->output,         ":utf8";
	binmode $b->failure_output, ":utf8";
	binmode $b->todo_output,    ":utf8";
	
	{
	no strict 'refs';
	*{ caller().'::parse_packet' } = \&parse_packet;
	*{ caller().'::packet_dump' } = \&packet_dump;
	*{ caller().'::xd' } = \&xd;
	}

	splice @_, 0, 1, 'Test::More';
	goto &{ Test::More->can('import') };
}

use Encode ();
sub parse_packet {
	my $raw = shift;
	 Encode::_utf8_off($raw);
	my @names = qw( set add and xor or str del ins );
	my %pk;
	( @pk{ qw(type len id) }, $raw) = unpack 'V3 a*', $raw;
	if ($pk{type} == 17) {
		( @pk{ qw( space index offset limit count ) }, $raw) = unpack 'V5 a*', $raw;
		for (1..$pk{count}) {
			my @tuple = unpack 'V/(w/a*) a*', $raw;
			$raw = pop @tuple;
			map $_ = "$_:s", @tuple;
			push @{ $pk{tuples} },\@tuple;
		}
	}
	elsif ($pk{type} == 13) {
		( @pk{ qw( space flags ) }, $raw) = unpack 'V2 a*', $raw;
		my @tuple = unpack 'V/(w/a*) a*', $raw;
		$raw = pop @tuple;
		map $_ = "$_:s", @tuple;
		$pk{tuple} = \@tuple;
	}
	elsif ($pk{type} == 21) {
		( @pk{ qw( space flags ) }, $raw) = unpack 'V2 a*', $raw;
		my @tuple = unpack 'V/(w/a*) a*', $raw;
		$raw = pop @tuple;
		map $_ = "$_:s", @tuple;
		$pk{tuple} = \@tuple;
	}
	elsif ($pk{type} == 19) {
		( @pk{ qw( space flags ) }, $raw) = unpack 'V2 a*', $raw;
		my @tuple = unpack 'V/(w/a*) a*', $raw;
		$raw = pop @tuple;
		map $_ = "$_:s", @tuple;
		$pk{tuple} = \@tuple;
		my $count;
		($count,$raw) = unpack 'V a*', $raw;
		my @ops;
		for (1..$count) {
			my @op = unpack '(V C w/a*) a*',$raw;
			$raw = pop @op;
			
			if ($op[1] == 1 or $op[1] == 2 or $op[1] == 3 or $op[1] == 4 ) {
				($op[2]) = unpack 'V', $op[2];
			}
			elsif ($op[1] == 5) {
				push @op, unpack 'w/a* w/a* w/a*',pop @op;
				$op[$_] = unpack 'V', $op[$_] for 2,3;
				$op[4] .= ':s';
			}
			elsif( $op[1] == 6) {
				pop @op if @op == 3 and $op[2] eq '';
			}
			else {
				$op[2] .= ':s';
			}
			$op[1] = $names[$op[1]];
			push @ops, \@op;
		}
		$pk{ops} = \@ops;
	}
	elsif ($pk{type} == 22) {
		( @pk{ qw( flags proc ) }, $raw) = unpack 'V w/a* a*', $raw;
		my @tuple = unpack 'V/(w/a*) a*', $raw;
		$raw = pop @tuple;
		map $_ = "$_:s", @tuple;
		$pk{tuple} = \@tuple;
	}
	$pk{trash} = $raw;
	return \%pk;
}

sub pint32($$) {
	printf "%s<%s>=%d ", $_[0],join(' ', unpack '(H2)*', pack('V',$_[1]) ), $_[1];
}
sub pvint($$) {
	printf "%s<%s>=%d ", $_[0],join(' ', unpack '(H2)*', pack('w',$_[1]) ), $_[1];
}
sub pdata($$) {
	printf "%s<%s> [%s]", $_[0], join(' ', unpack '(H2)*',$_[1]),
		join('', map { ord() < 127 && ord() < 32 ? '.' : $_ } $_[1] =~ /(.)/sg);
}

sub packet_dump {
	my $raw = shift;
	 Encode::_utf8_off($raw);
	my @names = qw( set add and xor or str del ins );
	my %pk;
	my $LS = "# ";
	( @pk{ qw(type len id) }, $raw) = unpack 'V3 a*', $raw;
	print $LS;
	printf "type<%s>=%d ", join(' ', unpack '(H2)*', pack('V',$pk{type}) ), $pk{type};
	printf "len<%s>=%d ", join(' ', unpack '(H2)*', pack('V',$pk{len}) ), $pk{len};
	printf "id<%s>=%d ", join(' ', unpack '(H2)*', pack('V',$pk{id}) ), $pk{id};
	if ($pk{type} == 17) {
		print " [SELECT:$pk{type}]\n";
		( @pk{ qw( space index offset limit count ) }, $raw) = unpack 'V5 a*', $raw;
		
		print $LS;
		pint32( $_ => $pk{$_} ) for qw(space index offset limit count);
		print "\n";
		my $d;
		for (1..$pk{count}) {
			
			my $count;
			($count,$raw) = unpack 'V a*', $raw;
			
			print "$LS\t";
			pint32( tuplesize => $count );
			print "\n";
		
			for (1..$count) {
				print "$LS\t\t";
				my ($s) = unpack 'w', $raw;
				pvint( len => $s );
				($d,$raw) = unpack 'w/a* a*', $raw;
				pdata( data => $d );
				print "\n";
			}
		}
	}
	elsif ($pk{type} == 13) {
		print " [INSERT:$pk{type}]\n";
		( @pk{ qw( space flags ) }, $raw) = unpack 'V2 a*', $raw;
		my @tuple = unpack 'V/(w/a*) a*', $raw;
		$raw = pop @tuple;
		map $_ = "$_:s", @tuple;
		$pk{tuple} = \@tuple;
	}
	elsif ($pk{type} == 21) {
		print " [DELETE:$pk{type}]\n";
		( @pk{ qw( space flags ) }, $raw) = unpack 'V2 a*', $raw;
		my @tuple = unpack 'V/(w/a*) a*', $raw;
		$raw = pop @tuple;
		map $_ = "$_:s", @tuple;
		$pk{tuple} = \@tuple;
	}
	elsif ($pk{type} == 19) {
		print " [UPDATE:$pk{type}]\n";
		( @pk{ qw( space flags ) }, $raw) = unpack 'V2 a*', $raw;
		print $LS;
		pint32( space => $pk{space} );
		pint32( flags => $pk{flags} );
		print "\n";
		
		print $LS;
		
		my ($size,$data,$d);
		($size,$raw) = unpack 'V a*', $raw;
		pint32( tuple => $size );
		print "\n";
		for (1..$size) {
			print "$LS\t";
			my ($s) = unpack 'w', $raw;
			pvint( len => $s );
			($d,$raw) = unpack 'w/a* a*', $raw;
			pdata( data => $d );
			print "\n";
		}
		my $count;
		($count,$raw) = unpack 'V a*', $raw;
		
		print $LS;
		pint32( opcount => $count );
		print "\n";
		
		for (1..$count) {
			no warnings;
			print "$LS\t";
			my ($fn,$op,$fl);
			return warn("Truncated packet") if length $raw < 6;
			($fn,$op,$fl,$raw) = unpack 'V C w a*', $raw;
			pint32( field => $fn );
			printf "op<%02x>=%d [%3s]  ", $op, $op, $names[$op];
			pvint( field_len => $fl );
			
			#pvint( field => $fn );
			my $field = substr($raw, 0, $fl,'');
			pdata( data => $field );
			if ($op == 5) {
				print "\n";
				
				print "$LS\t\t";
				($fl,$field) = unpack 'w a*', $field;
				pvint( off_len => $fl );
				if ($fl == 4) {
					($fn, $field) = unpack 'V a*', $field;
					pint32( offset => $fn);
				} else {
					pdata( data => substr($field, 0, $fl,'') );
				}
				print "\n";
				
				print "$LS\t\t";
				($fl,$field) = unpack 'w a*', $field;
				pvint( len_len => $fl );
				if ($fl == 4) {
					($fn, $field) = unpack 'V a*', $field;
					pint32( length => $fn);
				} else {
					pdata( data => substr($field, 0, $fl,'') );
				}
				print "\n";
				
				print "$LS\t\t";
				($fl,$field) = unpack 'w a*', $field;
				pvint( str_len => $fl );
				pdata( string => substr($field, 0, $fl,'') );
			}
			print "\n";
		}
	}
	elsif ($pk{type} == 22) {
		print " [CALL:$pk{type}]\n";
		( @pk{ qw( flags proc ) }, $raw) = unpack 'V w/a* a*', $raw;
		my @tuple = unpack 'V/(w/a*) a*', $raw;
		$raw = pop @tuple;
		map $_ = "$_:s", @tuple;
		$pk{tuple} = \@tuple;
	}
	$pk{trash} = $raw;
	return \%pk;
}

sub xd ($;$) {
	if( eval{ require Devel::Hexdump; 1 }) {
		no strict 'refs';
		*{ caller().'::xd' } = \&Devel::Hexdump::xd;
	} else {
		no strict 'refs';
		*{ caller().'::xd' } = sub($;$) {
			my @a = unpack '(H2)*', $_[0];
			my $s = '';
			for (0..$#a/16) {
				$s .= "@a[ $_*16 .. $_*16 + 7 ]  @a[ $_*16+8 .. $_*16 + 15 ]\n";
			}
			return $s;
		};
	}
	goto &{ caller().'::xd' };
}


1;
