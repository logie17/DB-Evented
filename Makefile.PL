use 5.006;
use strict;
use warnings FATAL => 'all';
use inc::Module::Install;

name     'DB-Evented';
all_from 'lib/DB/Evented.pm';
author   q{Logan Bell <logie@cpan.org>};
license  'artistic2';

perl_version 5.006;

tests_recursive('t');

resources (
   homepage   => 'http://www.shutterstock.com',
   license    => 'http://www.perlfoundation.org/artistic_license_2_0',
   repository => 'git://github.shuttercorp.net/lbell/DB-Evented.git',
   bugtracker => 'http://rt.cpan.org/NoAuth/Bugs.html?Dist=DB-Evented',
);

configure_requires (
   'Module::Install'      => 0,
	 'inc::Module::Install' => 0,
);

build_requires (
   'Test::Most'   => 0,
   'DBD::SQLite' => 0,
);

requires (
   'indirect'              => 0,
	 'multidimensional'      => 0,
	 'bareword::filehandles' => 0,
   'strictures'            => 0,
   'AnyEvent'              => 0,
   'AnyEvent::DBI'         => 0,
);

install_as_cpan;
auto_install;
WriteAll;
