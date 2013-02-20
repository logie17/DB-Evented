#!perl -T
use 5.006;
use strict;
use warnings FATAL => 'all';
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'DB::Evented' ) || print "Bail out!\n";
}

diag( "Testing DB::Evented $DB::Evented::VERSION, Perl $], $^X" );
