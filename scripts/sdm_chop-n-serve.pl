#!/usr/bin/perl
#
# Takes SDM, target dir, and a scan list, and creates an edited full
# SDM in targetdir.
#
# What it actually does is replace BDF names in Main.xml with 'X1',
# the indicator for "no BDF available". This effectively chops out the
# scans marked thus.
#
# Sarah B Spolaor July 2015
# Adapted from Bryan Butler's choose_SDM_scans.pl
#

use strict;

my $progname = 'sdm\_chop-n-serve.pl';

if ($#ARGV < 2) {
    print "\n\tUSAGE ERROR ($progname):\n\t sdm_chop-n-serve.pl inputSDM targetDir [scanList]
       inputSDM     SDM name including full path. Script will not change this SDM.
       targetDir    Where to put the edited SDM. Will make full copy of the SDM to (name)\_edited.
       scanList     (optional) examples: 1; 1~2; 1,3,5; 1,2,3~6\n";
    exit 1;
}

my $sdm_directory= $ARGV[0];
my $target_dir = $ARGV[1];

# Check SDM source directory
if (! -d $sdm_directory) {
    print "\n\tERROR ($progname):\n\tSDM directory not found.\n\n";
    exit 1;
}
if (!-e "$sdm_directory/Main.xml") {
    print "\n\tERROR ($progname):\n\tCouldn't find Main.xml in $sdm_directory\n\n";
    exit 2;
}
my @dummy = split('/',$sdm_directory);

# Check target dir
system "touch $target_dir/temp.garbage";
if (!-e "$target_dir/temp.garbage"){
    print "\n\tERROR ($progname):\n\tYou do not have write permission in requested target dir $target_dir\n\n";
    exit 4;
}
system "rm -f $target_dir/temp.garbage";

# Does target SDM already exist?
my $target_sdm = "$target_dir/".$dummy[-1]."\_edited";
if (-e "$target_sdm"){
    print "\n\tERROR ($progname):\n\tRequested target file $target_sdm already exists; we do not overwrite.\n\n";
    exit 3;
}

# Copy SDM to a dir we know we can edit in place.
# Do not copy any bdfpkls directory.
system "mkdir $target_sdm";
open(SDMDIR, "ls -1 $sdm_directory |");
while(<SDMDIR>){
    chomp;
    my $sdm_content = $_;
    if ($sdm_content !~ /bdfpkls/ && $sdm_content !~ /ASDMBinary/){
        system "cp -r $sdm_directory/$sdm_content $target_sdm";
	chmod 0777, "$target_sdm/$sdm_content";
    }
}
close(SDMDIR);
chmod 0777, "$target_sdm";


my $main_xml_filename = "$target_sdm/Main.xml";
system "rm $target_sdm/Main.xml";
open (OUTPUT, ">$main_xml_filename");

chdir $sdm_directory;
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
#print "\ndesired scans: @desired_scans\n";
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
	    last;
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

chmod 0777, $main_xml_filename;

exit 0;



