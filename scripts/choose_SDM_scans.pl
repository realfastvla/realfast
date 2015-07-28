#!/usr/bin/perl

use strict;

#if ($#ARGV < 1) {
#    print "usage: createSDM+BDF.pl filesetID testOrProd [scanList]\n";
#    print "       testOrProd = 1 => production\n";
#    print "       testOrProd = 0 => test\n";
#    print "       scanList (which is optional) examples: 1; 1~2; 1,3,5; 1,2,3~6\n";
#    exit 1;
#}
#my $filesetId = $ARGV[0];
#my $testOrProduction = $ARGV[1];

# print "Enter the SDM directory: ";
# my $sdm_directory= <STDIN>;
my $sdm_directory= $ARGV[0];
chomp $sdm_directory;
if (! -d $sdm_directory) {
    print "SDM directory not found.\n";
    exit 1;
}

# print "Enter the output filename (for the Main.xml copy): ";
# my $main_xml_filename = <STDIN>;
my $main_xml_filename = $ARGV[1];
chomp $main_xml_filename;
if (-e $main_xml_filename) {
    print "output file $main_xml_filename already exists.\n";
    exit 2;
}
open (OUTPUT, ">$main_xml_filename");

chdir $sdm_directory;
if (! -e 'Main.xml') {
    print "Main.xml not found in SDM directory $sdm_directory.\n";
    exit 3;
}
open (INPUT, '<Main.xml');

# print "Enter the scan list (casa format): ";
# my $scan_list = <STDIN>;
my $scan_list = $ARGV[2];
chomp $scan_list;

#
# figure out desired scans
#
my @scan_segments = split(',',$scan_list);
my @desired_scans = ();
foreach my $scan_segment ( @scan_segments ) {
    if (index($scan_segment,'~') >= 0) {
        my ($start_scan,$stop_scan) = split('~',$scan_segment);
        for my $ii ( $start_scan .. $stop_scan ) {
            push @desired_scans, $ii;
        }
    } else {
        push @desired_scans, $scan_segment;
    }
}
#print "\ndesired scans: $desired_scans[0]";
#foreach my $ii ( 1 .. scalar(@desired_scans)-1 ) {
#    print ", $desired_scans[$ii]";
#}
#print "\n";

#
# find the first <row> element
#
LOOP: while ( <INPUT> ) {
    while (index($_,'<row>') < 0) {
        print OUTPUT "$_";
        $_ = <INPUT> or last LOOP;
    }
#
# skip down to the scanNumber element
#
    while ( index($_,'<scanNumber>') < 0 ) {
        print OUTPUT "$_";
        $_ = <INPUT> or last LOOP;
    }
    my @Flds = split('\<', $_);
    my $scan_number = (split('\>',$Flds[1]))[1];
    my $is_desired_scan = 0;
    foreach my $scan ( @desired_scans ) {
        if ( $scan_number == $scan ) {
            $is_desired_scan = 1;
        }
    }
    print OUTPUT "$_";
    $_ = <INPUT> or last LOOP;
#
# skip down to the datasize element
#
    while ( index($_,'<dataSize>') < 0 ) {
        print OUTPUT "$_";
        $_ = <INPUT> or last LOOP;
    }
    if (! $is_desired_scan ) {
        $_ =~ s/\d//g;
        $_ =~ s/></>0</;
    }
#
# skip down to the EntityRef element
#
    while ( index($_,'<EntityRef ') < 0 ) {
        print OUTPUT "$_";
        $_ = <INPUT> or last LOOP;
    }
#
# if it's not already missing, or an undesired scan, put in 'X1' for the BDF ID
#
    if ( index($_,'X1') < 0 ) {
        my $entity_id = (split('"',$_))[1];
        if (! $is_desired_scan ) {
#           print "marking scan $scan_number as MISSING its BDF data...\n";
            $entity_id =~ /[0-9]/;
            my $bdf_id = substr($entity_id,$-[0]);
            $_ =~ s/$bdf_id/X1/;
            $_ =~ s/\/\/\//\/\//;
        } else {
#           print "marking scan $scan_number as HAVING its BDF data...\n";
        }
    } else {
#       print "scan $scan_number is missing its original BDF data...\n";
    }
    print OUTPUT "$_";
}
close INPUT;
close OUTPUT;


exit 0;



