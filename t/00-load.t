#!/usr/bin/env perl -w

use strict;
use lib::abs '../lib';
use Test::More tests => 2;
use Test::NoWarnings;

BEGIN {
	use_ok( 'Protocol::Tarantool' );
}

diag( "Testing Protocol::Tarantool $Protocol::Tarantool::VERSION, Perl $], $^X" );
exit;
require Test::NoWarnings;
