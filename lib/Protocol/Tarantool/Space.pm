package Protocol::Tarantool::Types;

our %TYPE = (
	STR       => [ 'p' ],
	NUM       => [ 'I' ],
	NUM64     => [ 'L' ],
	INT       => [ 'i' ],
	INT64     => [ 'l' ],
	UTF       => [ 'u' ],
	UTF8STR   => [ 'u' ],
	
#	I8        => [ '' ],
#	I16       => [ '' ],
#	I32       => [ '' ],
#	I64       => [ '' ],
#	U8        => [ '' ],
#	U16       => [ '' ],
#	U32       => [ '' ],
#	U64       => [ '' ],
);

package Protocol::Tarantool::Space;

use strict;
use Carp;
$Carp::Internal{ (__PACKAGE__) }++;
use JSON::XS ();


sub new {
    my ($class, $no, $space) = @_;
    croak 'space number must conform the regexp qr{^\d+}' unless defined $no and $no =~ /^\d+$/;
    croak "'fields' not defined in space hash" unless 'ARRAY' eq ref $space->{fields};
    $space->{indexes} = delete $space->{indices} if exists $space->{indices};
    croak "wrong 'indexes' hash" if $space->{indexes} and ref $space->{indexes} ne 'HASH';
    #!$space->{indexes} or 'HASH' ne ref $space->{indexes};

    my $name = $space->{name};
    croak 'wrong space name: ' . (defined($name) ? $name : 'undef') unless $name and $name =~ /^[a-z_]\w*$/i;


    #my $fqr = qr{^(?:STR|NUM|NUM64|INT|INT64|UTF8STR|JSON|MONEY|BIGMONEY)$};
    #my $fqr = qr{^(?:STR|NUM|NUM64|INT|INT64|UTF|UTF8STR)$};

    my (@fields, %fast, $default_type);
    $default_type = $space->{default_type} || 'STR';
    croak "wrong 'default_type'=$space->{default_type}" unless exists $Protocol::Tarantool::Types::TYPE{ $default_type };
    
    my $unpack = '';
    my @names;
    for (my $no = 0; $no < @{ $space->{fields} }; $no++) {
        my $f = $space->{ fields }[ $no ];
        my ($name,$type,$pack);

        if (ref $f eq 'HASH') {
            $name = $f->{name} || "f$no";
            $type = $f->{type};
        }
        elsif(ref $f) {
            croak 'wrong field name or description';
        }
        else {
            $name = $f;
            $type = exists $space->{types}[$no] ? $space->{types}[$no] : $default_type;
        }
        
        $type = uc $type;
        
        
        exists $Protocol::Tarantool::Types::TYPE{ $type }
            or croak 'unknown field type: ' . (defined($type) ? $type : 'undef');

        push @fields,  $f = {
            name    => $name,
            no      => $no,
            idx     => $no,
            type    => $type,
            pack    => $Protocol::Tarantool::Types::TYPE{ $type },
        };
        push @names, $name;

        # TODO: unpack format
        $unpack .= $Protocol::Tarantool::Types::TYPE{ $type }[0];
        

        croak 'wrong field name: ' .
            (defined($name) ? $name : 'undef')
                unless $name and $name =~ /^[a-z_]\w*$/i;

        croak "Duplicate field name: $f->{name}" if exists $fast{ $f->{name} };
        $fast{ $f->{name} } = $f;
    }

    my %indexes;
    if ($space->{indexes}) {
        for my $no (keys %{ $space->{indexes} }) {
            my $l = $space->{indexes}{ $no };
            croak "wrong index number: $no" unless $no =~ /^\d+$/;

            my ($name, $fields);

            if ('ARRAY' eq ref $l) {
                #$name = "i$no";
                $fields = $l;
            } elsif ('HASH' eq ref $l) {
                $name = $l->{name};
                $fields = [ ref($l->{fields}) ? @{ $l->{fields} } : $l->{fields} ];
            } else {
                #$name = "i$no";
                $fields = [ $l ];
            }

            croak "wrong index name: `$name' for index[$no]" if length $name and $name !~ /^[a-z_]\w*$/i;

            for (@$fields) {
                croak "field '$_' is present in index but isn't in fields" unless exists $fast{ $_ };
            }

            $indexes{ $no } = {
                no      => $no,
                name    => $name,
                fields  => $fields
            };
            
            $indexes{ $name } = $indexes{ $no } if length $name;

        }
    }

#    my $tuple_class = 'DR::Tarantool::Tuple::Instance' .
#        Digest::MD5::md5_hex( join "\0", sort keys %fast );

    bless {
        fields          => \@fields,
        fast            => \%fast,
        name            => $name,
        number          => $no,
        no              => $no,
        default_type    => $default_type,
        indexes         => \%indexes,
#        tuple_class     => $tuple_class,
		fnames           => \@names,
        unpack          => $unpack,
        default_unpack  => $Protocol::Tarantool::Types::TYPE{ $default_type },
    } => ref($class) || $class;

}

sub index : method {
	my ($self, $index) = @_;
	return $self->{indexes}{ $index } if exists $self->{indexes}{ $index };
	return $self->{$index->{no}} if ref $index and exists $self->{indexes}{ $index->{no} };
	croak "Index '$index' is not defined. Available indices: [@{[ sort keys %{ $self->{indexes} } ]}]";
}

# must return AoA, where inner array(s) contain key for single record

sub keys : method {
    my ($self, $keys, $idx, $disable_warn) = @_;

    $idx = $self->index($idx) unless ref $idx;
    
    my $ksize = @{ $idx->{fields} };

    $keys = [[ $keys ]] unless ref $keys eq 'ARRAY';
    
    unless ( ref $keys->[0] eq 'ARRAY' ) {
        if ($ksize == @$keys) {
            $keys = [ $keys ];
            carp "Ambiguous keys list (it was used as ONE key), ".
                    "Use brackets to solve the trouble."
                        if $ksize > 1 and !$disable_warn;
        } else {
            $keys = [ map {
                croak "key must have less than or equal than $ksize elements" if @$_ > $ksize;
                [ $_ ]
            } @$keys ];
        }
    }
    my $format = '';
    for (@{ $idx->{fields} }) {
        #$idx->{fields}[$i]
        $format .= $self->{fast}{ $_ }{ pack }[0];
    }
    return $keys, $format;
}

our %UPDATE = (
	delete => '#',
	del    => '#',
	set    => '=',
	insert => '!',
	ins    => '!',
	add    => '+',
	sub    => '-',
	and    => '&',
	or     => '|',
	xor    => '^',
	substr => ':',
);

our %OPS = (
	
); @OPS{ values %UPDATE } = (1)x keys %UPDATE;

sub updates {
	my ($self,$ops) = @_;
	my @rv;
	for my $fld ( @$ops ) {
		ref $fld eq 'ARRAY' or croak "Operation list must be AoA";
		my ($fn,$op,@rest) = @$fld;
		if (length $op == 1) {
			croak "Unknown operation: '$op'. Available ops: [@{[ keys %OPS ]}]"
				unless exists $OPS{$op};
		} else {
			croak "Unknown operation: '$op'. Available ops: [@{[ keys %OPS ]}] or aliases: [@{[ keys %UPDATE ]}]"
				unless exists $UPDATE{$op};
				$op = $UPDATE{$op};
		}
		my $f = $self->{fast}{ $fn } or croak "Unknown field `$fn'";
		my $format = $f->{ pack }[0];
			
		push @rv, [
			$f->{no}, $op,
			$op eq '#' ? () :
			$op eq ':' ? ( $rest[0],$rest[1] ) :
			$rest[0],
			$format
		];
	}
	use uni::perl ':dumper';
	warn dumper \@rv;
	return \@rv;
}

sub tuple {
	my ($self,$src) = @_;
	my $av;
	if (ref $src eq 'HASH') {
		$av = [];
		my $left = keys %$src;
		for ( @{ $self->{fnames} } ) {
			push @$av, exists $src->{$_} ? exists $src->{$_} : undef;
			--$left or last;
		}
		if ($left > 0) {
			my %src = %$src;
			delete @src{ @{ $self->{fnames} } };
			croak("Unknown fields in tuple hash: [@{[ keys %src ]}] ");
		}
	}
	elsif (ref $src eq 'ARRAY') {
		$av = $src;
	}
	else {
		croak "Unknown reference for tuple data: $src";
	}
	return wantarray ? ($av, $self->{unpack}) : $av;;
}


1;

__END__


=head2 tuple_class

Creates (or returns) class for storage tuples. The class will be child of
L<DR::Tarantool::Tuple>. Returns unique class (package) name. If package
is already exists, the method won't recreate it.

=cut

sub tuple_class {
    my ($self) = @_;
    my $class = $self->{tuple_class};


    no strict 'refs';
    return $class if ${ $class . '::CREATED' };

    die unless eval "package $class; use base 'DR::Tarantool::Tuple'; 1";

    for my $fname (keys %{ $self->{fast} }) {
        my $fnumber = $self->{fast}{$fname};

        *{ $class . '::' . $fname } = eval "sub { \$_[0]->raw($fnumber) }";
    }

    ${ $class . '::CREATED' } = time;

    return $class;
}


=head2 name

returns space name

=cut

sub name { $_[0]{name} }


=head2 number

returns space number

=cut

sub number { $_[0]{number} }

sub _field {
    my ($self, $field) = @_;

    croak 'field name or number is not defined' unless defined $field;
    if ($field =~ /^\d+$/) {
        return $self->{fields}[ $field ] if $field < @{ $self->{fields} };
        return undef;
    }
    croak "field with name '$field' is not defined in this space"
        unless exists $self->{fast}{$field};
    return $self->{fields}[ $self->{fast}{$field} ];
}


=head2 field_number

Returns number of field by its name.

=cut

sub field_number {
    my ($self, $field) = @_;
    croak 'field name or number is not defined' unless defined $field;
    return $self->{fast}{$field} if exists $self->{fast}{$field};
    croak "Can't find field '$field' in this space";
}


=head2 tail_index

Returns index of the first element that is not described in the space.

=cut

sub tail_index {
    my ($self) = @_;
    return scalar @{ $self->{fields} };
}


=head2 pack_field

packs field before making database request

=cut

sub pack_field {
    my ($self, $field, $value) = @_;
    croak q{Usage: $space->pack_field('field', $value)}
        unless @_ == 3;

    my $f = $self->_field($field);

    my $type = $f ? $f->{type} : $self->{default_type};

    if ($type eq 'JSON') {
        my $v = eval { JSON::XS->new->allow_nonref->utf8->encode( $value ) };
        croak "Can't pack json: $@" if $@;
        return $v;
    }

    my $v = $value;
    utf8::encode( $v ) if utf8::is_utf8( $v );
    return $v if $type eq 'STR' or $type eq 'UTF8STR';
    return pack "L$LE" => $v if $type eq 'NUM';
    return pack "l$LE" => $v if $type eq 'INT';
    return pack "Q$LE" => $v if $type eq 'NUM64';
    return pack "q$LE" => $v if $type eq 'INT64';

    if ($type eq 'MONEY' or $type eq 'BIGMONEY') {
        my ($r, $k) = split /\./, $v;
        for ($k) {
            $_ = '.00' unless defined $_;
            s/^\.//;
            $_ .= '0' if length $_ < 2;
            $_ = substr $_, 0, 2;
        }
        $r ||= 0;

        if ($r < 0) {
            $v = $r * 100 - $k;
        } else {
            $v = $r * 100 + $k;
        }

        return pack "l$LE", $v if $type eq 'MONEY';
        return pack "q$LE", $v;
    }


    croak 'Unknown field type:' . $type;
}


=head2 unpack_field

unpacks field after extracting data from database

=cut

sub unpack_field {
    my ($self, $field, $value) = @_;
    croak q{Usage: $space->pack_field('field', $value)}
        unless @_ == 3;

    my $f = $self->_field($field);

    my $type = $f ? $f->{type} : $self->{default_type};

    my $v = $value;
    utf8::encode( $v ) if utf8::is_utf8( $v );

    if ($type eq 'JSON') {
        return $v unless length $v;
        $v = JSON::XS->new->allow_nonref->utf8->decode( $v );
        croak "Can't unpack json: $@" if $@;
        return $v;
    }

    $v = unpack "L$LE" => $v  if $type eq 'NUM';
    $v = unpack "l$LE" => $v  if $type eq 'INT';
    $v = unpack "Q$LE" => $v  if $type eq 'NUM64';
    $v = unpack "q$LE" => $v  if $type eq 'INT64';
    utf8::decode( $v )      if $type eq 'UTF8STR';
    if ($type eq 'MONEY' or $type eq 'BIGMONEY') {
        $v = unpack "l$LE" => $v if $type eq 'MONEY';
        $v = unpack "q$LE" => $v if $type eq 'BIGMONEY';
        my $s = '';
        if ($v < 0) {
            $v = -$v;
            $s = '-';
        }
        my $k = $v % 100;
        my $r = ($v - $k) / 100;
        $v = sprintf '%s%d.%02d', $s, $r, $k;
    }
    return $v;
}


=head2 pack_tuple

packs tuple before making database request

=cut

sub pack_tuple {
    my ($self, $tuple) = @_;
    croak 'tuple must be ARRAYREF' unless 'ARRAY' eq ref $tuple;
    my @res;
    for (my $i = 0; $i < @$tuple; $i++) {
        push @res => $self->pack_field($i, $tuple->[ $i ]);
    }
    return \@res;
}


=head2 unpack_tuple

unpacks tuple after extracting data from database

=cut

sub unpack_tuple {
    my ($self, $tuple) = @_;
    croak 'tuple must be ARRAYREF' unless 'ARRAY' eq ref $tuple;
    my @res;
    for (my $i = 0; $i < @$tuple; $i++) {
        push @res => $self->unpack_field($i, $tuple->[ $i ]);
    }
    return \@res;
}


sub _index {
}


=head2 index_number

returns index number by its name.

=cut

sub index_number {
    my ($self, $idx) = @_;
    croak "index name is undefined" unless defined $idx;
    return $self->_index( $idx )->{no};
}


=head2 index_name

returns index name by its number.

=cut

sub index_name {
    my ($self, $idx) = @_;
    croak "index number is undefined" unless defined $idx;
    return $self->_index( $idx )->{name};
}


sub pack_keys {
    my ($self, $keys, $idx, $disable_warn) = @_;

    $idx = $self->_index($idx);
    my $ksize = @{ $idx->{fields} };

    $keys = [[ $keys ]] unless 'ARRAY' eq ref $keys;
    unless('ARRAY' eq ref $keys->[0]) {
        if ($ksize == @$keys) {
            $keys = [ $keys ];
            carp "Ambiguous keys list (it was used as ONE key), ".
                    "Use brackets to solve the trouble."
                        if $ksize > 1 and !$disable_warn;
        } else {
            $keys = [ map { [ $_ ] } @$keys ];
        }
    }

    my @res;
    for my $k (@$keys) {
        croak "key must have $ksize elements" unless $ksize >= @$k;
        my @packed;
        for (my $i = 0; $i < @$k; $i++) {
            my $f = $self->_field($idx->{fields}[$i]);
            push @packed => $self->pack_field($f->{name}, $k->[$i])
        }
        push @res => \@packed;
    }
    return \@res;
}

sub pack_primary_key {
    my ($self, $key) = @_;

    croak 'wrong key format'
        if 'ARRAY' eq ref $key and 'ARRAY' eq ref $key->[0];

    my $t = $self->pack_keys($key, 0, 1);
    return $t->[0];
}

sub pack_operation {
    my ($self, $op) = @_;
    croak 'wrong operation' unless 'ARRAY' eq ref $op and @$op > 1;

    my $fno = $op->[0];
    my $opname = $op->[1];

    my $f = $self->_field($fno);

    if ($opname eq 'delete') {
        croak 'wrong operation' unless @$op == 2;
        return [ $f->{idx} => $opname ];
    }

    if ($opname =~ /^(?:set|insert|add|and|or|xor)$/) {
        croak 'wrong operation' unless @$op == 3;
        return [ $f->{idx} => $opname, $self->pack_field($fno, $op->[2]) ];
    }

    if ($opname eq 'substr') {
        croak 'wrong operation11' unless @$op >= 4;
        croak 'wrong offset in substr operation' unless $op->[2] =~ /^\d+$/;
        croak 'wrong length in substr operation' unless $op->[3] =~ /^\d+$/;
        return [ $f->{idx}, $opname, $op->[2], $op->[3], $op->[4] ];
    }
    croak "unknown operation: $opname";
}

sub pack_operations {
    my ($self, $ops) = @_;

    croak 'wrong operation' unless 'ARRAY' eq ref $ops and @$ops >= 1;
    $ops = [ $ops ] unless 'ARRAY' eq ref $ops->[ 0 ];

    my @res;
    push @res => $self->pack_operation( $_ ) for @$ops;
    return \@res;
}

=head1 COPYRIGHT AND LICENSE

 Copyright (C) 2011 Dmitry E. Oboukhov <unera@debian.org>
 Copyright (C) 2011 Roman V. Nikolaev <rshadow@rambler.ru>

 This program is free software, you can redistribute it and/or
 modify it under the terms of the Artistic License.

=head1 VCS

The project is placed git repo on github:
L<https://github.com/unera/dr-tarantool/>.

=cut

1;
