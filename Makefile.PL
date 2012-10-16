use 5.008008;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

my $coflag = ( $Config{ccname} eq 'cl' ) ? '/Fo' : '-o';

sub MY::c_o
{
	my $t = shift->MM::c_o(@_);
	#die "$t";
	unless ( $t =~ /.c\$\(OBJ_EXT\):\n\t.*\$\*\$\(OBJ_EXT\)/ ) {
		$t =~ s/(\.c\$\(OBJ_EXT\):\n\t.*)/$1 $coflag \$*\$(OBJ_EXT)/;
	}
	return $t;
}

WriteMakefile(
	NAME              => 'Protocol::Tarantool',
	VERSION_FROM      => 'lib/Protocol/Tarantool.pm', # finds $VERSION
	PREREQ_PM         => {}, # e.g., Module::Name => 1.1
	ABSTRACT_FROM  => 'lib/Protocol/Tarantool.pm', # retrieve abstract from module
	AUTHOR         => 'Mons Anderson <mons@coan.org>',
#    LIBS              => ['-ltarantool'], # e.g., '-lm'
	DEFINE            => '', # e.g., '-DHAVE_SOMETHING'
	INC               => '-I.', # e.g., '-I. -I/usr/include/other'
	# Un-comment this if you add C files to link with later:
	C                 => [ <connector/c/*.c> ],
	OBJECT            => 'Tarantool.o $(O_FILES)', # link all the C files too
);
