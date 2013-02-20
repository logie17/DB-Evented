#!/usr/bin/env perl

use strict;
use warnings qw(all);

use File::Temp qw(tempfile);
use AnyEvent;
use DB::Evented;

use Test::Most tests => 2;

my ($dh, $dname) = tempfile;
close $dh;

my $evented = DB::Evented->new("DBI:SQLite2:dbname=$dname", "","");

my $dbh = $evented->any_event_handler;
my ($error, $result);
my $cv = AnyEvent->condvar;
$dbh->exec('create table test (test1 int, test2 varchar(200))',sub {return $cv->send($@) unless $_[1];$cv->send(undef,$_[1])});
($error,$result) = $cv->recv();
$dbh->exec('insert into test values (1, "foobar")',sub {return $cv->send($@) unless $_[1];$cv->send(undef,$_[1])});
($error,$result) = $cv->recv();
ok(!$error,'No errors creating a table');

my $results;
$evented->selectcol_arrayref(
  q{
    select
      test1,
      test2
    from
      test
  },
  { 
    Columns => [1,2],
    response => sub {
        $results->{result1} = shift;
    }		
  }
);

$evented->selectrow_hashref(
  q{
    select
      test1,
      test2
    from
      test
  },
  {
    response => sub {
      $results->{result2} = shift;
    }
  }
);

$evented->execute_in_parallel;

is_deeply $results, { 'result2' => { 'test1' => '1', 'test2' => 'foobar' }, 'result1' => [ '1', 'foobar' ] }, "Parallel results come back with data";

END {
  unlink $dname;
}

1;
