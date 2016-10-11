#!/usr/env perl
use strict;
use File::Slurp;

my $db1 = shift;
my $db2 = shift;
my $script = shift;
my $mysql = '/usr/local/mysql/bin/mysql --user=root';
$script = '/dev/stdin' unless $script;

die "Usage: $ARGV[0] <db1> <db2> <script>\n"
  unless ($db1 and $db2);

my $line = 0;
open(my $fh, '<:encoding(UTF-8)', $script);
while (<$fh>) {
  $line++;
  chomp;

  # MySQL log format? cut off timestamps, skip "Prepare" statements
  if (/^\d\d\d\d-\d\d-........:..........   .. (.*?)\t(.*)/) {
    next if ($1 eq 'Prepare');
    $_ = $2;
  }

  my $cmd = $_;

  # send $cmd to both mysql instances
  my $output1 = `echo "$cmd" | $mysql $db1 2>/tmp/db1`;
  my $output2 = `echo "$cmd" | $mysql $db2 2>/tmp/db2`;

  # compare the results
  if ($output1 ne $output2) {
    die "mismatch in line $line:\n\ncommand: $cmd\ndb1:\n$output1\ndb2: $output2\n";
  }

  my $text1 = read_file('/tmp/db1');
  my $text2 = read_file('/tmp/db2');

  if ($text1 ne $text1) {
    die "mismatch in line $line:\n\ncommand: $cmd\ndb1:\n$text1\ndb2: $text1\n";
  }

  print ".";
}
close($fh);

print "\n";
