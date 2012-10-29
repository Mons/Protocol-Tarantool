use 5.008008;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

WriteMakefile(
	NAME              => 'Protocol::Tarantool',
	VERSION_FROM      => 'lib/Protocol/Tarantool.pm', # finds $VERSION
	PREREQ_PM         => {}, # e.g., Module::Name => 1.1
	ABSTRACT_FROM  => 'lib/Protocol/Tarantool.pm', # retrieve abstract from module
	AUTHOR         => 'Mons Anderson <mons@cpan.org>',
#    LIBS              => ['-ltarantool'], # e.g., '-lm'
-e '.git' ? (
	DEFINE            => '-Wall -O0', # e.g., '-DHAVE_SOMETHING'
) : (
),
	INC               => '-I.', # e.g., '-I. -I/usr/include/other'
	# Un-comment this if you add C files to link with later:
);
