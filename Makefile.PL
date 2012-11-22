use strict;
use 5.008008;

use ExtUtils::MakeMaker;

WriteMakefile(
	NAME              => 'Protocol::Tarantool',
	VERSION_FROM      => 'lib/Protocol/Tarantool.pm', # finds $VERSION
	PREREQ_PM         => {}, # e.g., Module::Name => 1.1
	ABSTRACT_FROM  => 'lib/Protocol/Tarantool.pm', # retrieve abstract from module
	AUTHOR         => 'Mons Anderson <mons@cpan.org>',
	LICENSE           => 'perl',
#    LIBS              => ['-ltarantool'], # e.g., '-lm'
        (-e '.git' and !$ENV{USE_PRODUCTION_FLAGS}) ? (
                DEFINE            => '-std=c99 -Wall -Wno-declaration-after-statement -O0 -fno-omit-frame-pointer',
        ) : (
                DEFINE            => '-std=c99 -Wno-declaration-after-statement -O2 -fomit-frame-pointer',
        ),
	INC               => '-I.', # e.g., '-I. -I/usr/include/other'
	# Un-comment this if you add C files to link with later:
);
