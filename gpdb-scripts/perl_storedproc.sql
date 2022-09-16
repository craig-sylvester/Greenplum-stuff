select version();
set search_path to public;

DROP TABLE if exists perl_test cascade;
CREATE TABLE perl_test (
    i int,
    v varchar
) DISTRIBUTED RANDOMLY;

INSERT INTO perl_test (i, v)
   VALUES (1, 'first line'),
          (2, 'second line'),
          (3, 'third line'),
          (4, 'immortal');

CREATE OR REPLACE FUNCTION test_munge() RETURNS SETOF perl_test AS $$
    my $rv = spi_exec_query('select i, v from perl_test;');
    my $status = $rv->{status};
    my $nrows = $rv->{processed};
    foreach my $rn (0 .. $nrows - 1) {
        my $row = $rv->{rows}[$rn];
        $row->{i} += 200 if defined($row->{i});
        $row->{v} =~ tr/A-Za-z/a-zA-Z/ if (defined($row->{v}));
        return_next($row);
    }
    return undef;
$$ LANGUAGE plperl;

SELECT * FROM test_munge();
