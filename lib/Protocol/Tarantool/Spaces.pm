package Protocol::Tarantool::Spaces;

use strict;

use Carp;
$Carp::Internal{ (__PACKAGE__) }++;

use Protocol::Tarantool::Space;

use uni::perl ':dumper';

sub new {
	my $pk = shift;
	my $ref = shift || {};
	my $self = bless $ref, $pk;
	for my $sno (keys %$ref) {
		$ref->{$sno} = Protocol::Tarantool::Space->new( $sno, $ref->{$sno} );
		$ref->{$ref->{$sno}->{name}} = $ref->{$sno};
	}
	#warn dumper $ref;
	$self;
}

sub space {
	my ($self, $space) = @_;
	#croak 'space name or number is not defined' unless defined $space;
	return $self->{ $space } if exists $self->{$space};
	return $self->{$space->{no}} if ref $space and exists $self->{$space->{no}};
	croak "space '$space' is not defined. Available spaces: [@{[ sort keys %{ $self->{spaces} } ]}]";
}

1;
