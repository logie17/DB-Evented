package DB::Evented;

use 5.006;
use strictures;
use AnyEvent::DBI;

=head1 NAME

DB::Evented - A pragmatic DBI like evented module.

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';
our $handlers = [];

=head1 SYNOPSIS

Doing selects in synchronise order is not always the most efficient way to interact with the 
Database. 

  use DB::Evented;

  my $evented = DB::Evented->new("DBI:SQLite2:dbname=$dname", "","");

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

=head1 STATIC METHODS

=head2 new ($connection_str, $username, $pass, %other_anyevent_dbi_params )

In order to initialize a DB::Evented object a connection_str is most likely required.
See AnyEvent::DBI for more information.

=cut

sub new {
  my $class = shift;
  $class ||= ref $class;
  return bless {
    connection_str => $_[0],
    username => $_[1],
    pass => $_[2],
    _queue => [],
  }, $class;
}

=head1 INSTANCE METHODS

=head2 any_event_handler

This will return an AnyEvent::DBI handler. The key difference between this handler and DBI is that it's using AnyEvent
under the hood. What does this mean? It means that if you use an AnyEvent::DBI method it will run asynchronously.

=cut
sub any_event_handler {
  my $self = shift;
  return AnyEvent::DBI->new($self->{connection_str}, $self->{username}, $self->{pass}, on_error => sub {
    $self->clear_queue;
    die "DBI Error: $@ at $_[1]:$_[2]\n";
  });
}

=head2 clear_queue

Clears the queue of any db todos

=cut

sub clear_queue {
  $_[0]->{_queue} = undef;
}

=head2 execute_in_parallel

Will execute all of the queued statements in parallel. This will create a pool of handlers and cache them if necessary.

=cut

sub execute_in_parallel {
  my $self = shift;
  if ( scalar @{$self->{_queue}} ) {
    # Setup a pool of handlers
    # TODO: Make this more intelligent to shrink
    unless ( scalar @{$handlers} || ( @{$handlers} > @{$self->{_queue}} )) {
      for my $item (@{$self->{_queue}} ) {
        push @{$handlers}, $self->any_event_handler;
      }
    }
    $self->{cv} = AnyEvent->condvar;
    my %handlers;
    my $count = 0;
    for my $item ( @{$self->{_queue}} ) {
      my $cb = pop @$item;
      my $callback_wrapper = sub { 
        my ($dbh, $result) = @_;
        $cb->($result, $dbh);
        $self->{cv}->end;
      };
      my $req_method = pop @$item;
      $self->{cv}->begin;
      AnyEvent::DBI::_req($handlers->[$count], $callback_wrapper, (caller)[1,2], $req_method, @$item);
      $count++;
    }
    $self->{cv}->recv;
    delete $self->{cv};
    $self->clear_queue;
  }
}

sub _add_to_queue {
  my ( $self, $sql, $attr, $key_field, @args) = @_;

  my $cb = delete $attr->{response};
  my $item = [$sql, $attr, $key_field, @args, __PACKAGE__ . '::_req_dispatch', $cb]; 

  push @{$self->{_queue}}, $item;
}

sub _req_dispatch {
  my (undef, $st, $attr, $key_field, @args) = @{+shift};
  my $method_name = pop @args;
  my $result = $AnyEvent::DBI::DBH->$method_name($key_field ? ($st, $key_field, $attr, @args) : ($st, $attr, @args) )
    or die [$DBI::errstr];

  [1, $result ? $result : undef];
}

=head2 selectall_arrayref ($sql, \%attr, @binds )

This method functions in the same way as DBI::selectall_arrayref. The key difference
being it delays the execution until execute_in_parallel has been called. The results
can be accessed in the response attribute call back 

=cut

=head2 selectall_hashref ($sql, $key_field, \%attr, @binds )

This method functions in the same way as DBI::selectall_hashref. The key difference
being it delays the execution until execute_in_parallel has been called. The results
can be accessed in the response attribute call back 

=cut

=head2 selectrow_arrayref ($sql, \%attr, @binds )

This method functions in the same way as DBI::selectrow_arrayref. The key difference
being it delays the execution until execute_in_parallel has been called. The results
can be accessed in the response attribute call back 

=cut

=head2 selectrow_hashref ($sql, \%attr, @binds )

This method functions in the same way as DBI::selectrow_hashref. The key difference
being it delays the execution until execute_in_parallel has been called. The results
can be accessed in the response attribute call back 

=cut

for my $method_name ( qw(selectrow_hashref selectcol_arrayref selectall_hashref selectall_arrayref) ) {
  no strict 'refs';
  *{$method_name} = sub {
    my $self = shift;
    my ($sql, $key_field, $attr, @args) = (shift, ($method_name eq 'selectall_hashref' ? (shift) : (undef)), shift, @_);
    $self->_add_to_queue($sql, $attr, $key_field, @args, $method_name);
  };
}

=head1 AUTHOR

Logan Bell, C<< <logie at cpan.org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-shutterstock-db-evented at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DB-Evented>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DB::Evented

You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=DB-Evented>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/DB-Evented>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/DB-Evented>

=item * Search CPAN

L<http://search.cpan.org/dist/DB-Evented/>

=back

=head1 ACKNOWLEDGEMENTS

Mom, Aaron Cohen, and Belden Lyman and thanks to Shutterstock for allowing me to create this module.

=head1 LICENSE AND COPYRIGHT

Copyright 2013 Logan Bell.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

Any use, modification, and distribution of the Standard or Modified
Versions is governed by this Artistic License. By using, modifying or
distributing the Package, you accept this license. Do not use, modify,
or distribute the Package, if you do not accept this license.

If your Modified Version has been derived from a Modified Version made
by someone other than you, you are nevertheless required to ensure that
your Modified Version complies with the requirements of this license.

This license does not grant you the right to use any trademark, service
mark, tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge
patent license to make, have made, use, offer to sell, sell, import and
otherwise transfer the Package with respect to any patent claims
licensable by the Copyright Holder that are necessarily infringed by the
Package. If you institute patent litigation (including a cross-claim or
counterclaim) against any party alleging that the Package constitutes
direct or contributory patent infringement, then this Artistic License
to you shall terminate on the date that such litigation is filed.

Disclaimer of Warranty: THE PACKAGE IS PROVIDED BY THE COPYRIGHT HOLDER
AND CONTRIBUTORS "AS IS' AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, OR NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY
YOUR LOCAL LAW. UNLESS REQUIRED BY LAW, NO COPYRIGHT HOLDER OR
CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE OF THE PACKAGE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


=cut

1; # End of DB::Evented
