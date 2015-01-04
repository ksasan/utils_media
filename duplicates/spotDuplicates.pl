#!perl.exe

# ----------------------------------------------------------------------
# 
# Still ToDo:
# 1. Chdir to deeply nested directory fails. Need to fix it!
# 2. Extra set of ||| at end of hst file
# ----------------------------------------------------------------------

use strict;
use File::Find;
use File::Spec;
use Digest::MD5;
use Data::Dumper;

my %files;
my %md5;
my %hist;
my $wasted = 0;
my %configSetting = (
	"maxSize" => 5.0e4,
	#"maxSize" => 7.0e7,
          "outFile" => "duplicateFiles.list",
          "recSep" => '|',
          "historyFile" => "checksumHistory.hst",
	  "backupWithTimeStamp" => 1,
     );



print "Processing files ....\n" ;
if(@ARGV) {
	find(\&check_file, @ARGV );
} else {
	find(\&check_file, ".");
}

&loadHistoryFile();
&createDuplicateList();
&updateHistoryFile();

sub loadHistoryFile() {
    local $" = ", ";
    my $recSep = quotemeta($configSetting{"recSep"});

    print "Processing History files ....\n" ;
    open(HISTFILE, $configSetting{"historyFile"}) || return ;
    while(<HISTFILE>) {
     chomp;
     my($md5hash, $size, $mtime, $fullPath) ;
     ($md5hash, $size, $mtime, $fullPath) = split($recSep, $_);
     next if(exists($hist{$fullPath}));
     $hist{$fullPath} = {};
     $hist{$fullPath}{"MTIME"} = $mtime ;
     $hist{$fullPath}{"md5hash"} = $md5hash ;
     $hist{$fullPath}{"size"} = $size ;
    }
    close(HISTFILE);
}
#
# Yet to be completed (TODO)
#
sub getHashFiles {
    my(@files, @hashFiles) = @_ ;
     foreach my $file (@{$files{$size}}) {
         open(FILE, $file) or next;
         binmode(FILE);
         my $md5hash = &getUpdatedHash($file);
	 push(@hashFiles, $md5hash);
         close(FILE);
     }
}

sub createDuplicateList() {
    local $" = ", ";
    foreach my $size (sort {$b <=> $a} keys %files) {
     next unless @{$files{$size}} > 1;
     # Scaling Operation => Can utilize multi processing here !!
     foreach my $file (@{$files{$size}}) {
         open(FILE, $file) or next;
         binmode(FILE);
         my $md5hash = &getUpdatedHash($file);
         #my $md5hash = Digest::MD5->new->addfile(*FILE)->hexdigest ;
         if( exists($md5{$md5hash})) {
          push @{$md5{$md5hash}{"files"}}, $file;
         } else {
          $md5{$md5hash} = {} ;
          $md5{$md5hash}{"size"} = $size;
          $md5{$md5hash}{"checksum"} = $md5hash;
          push @{$md5{$md5hash}{"files"}},$file;
         }
         #push @{$md5{$md5hash}},$file;
         close(FILE);
     }
    }
    foreach my $hash (keys %md5) {
     next unless @{$md5{$hash}{"files"}} > 1;
     my $size = $md5{$hash}{"size"};
     print "$size: " . join(" ", @{$md5{$hash}{"files"}}) . "\n";
     $wasted += $size * (@{$md5{$hash}{"files"}} - 1);
    }
    #print Dumper(\%md5);
    &dumpFileList();

    1 while $wasted =~ s/^([-+]?\d+)(\d{3})/$1,$2/;
    print "$wasted bytes in duplicated files\n";

}

sub dumpFileList {
    my @records = ();
    my @filesList = ();
    my $recSep = $configSetting{"recSep"};

    foreach my $checkSum (keys %md5) {
     my @pathList = () ;
     my @allFiles = (@{$md5{$checkSum}{"files"}}) ;
     if(@allFiles == 1) {
	     #print "Found no duplicate for hash $checkSum \n" ;
	     # No duplicate file actually exists for this checksum and possibly multiple files of same size exists
	     # This can be further expanded to check in case file name and creation date are kind of same, there may be additional logic required here!!
	     next ;
     }
     foreach my $fileName (@{$md5{$checkSum}{"files"}}) { push(@pathList, File::Spec->rel2abs($fileName)); }
     push(@records, sprintf("%d%s%s%s%d%s%s", scalar(@pathList), $recSep, $checkSum, $recSep, $md5{$checkSum}{"size"}, $recSep, join($recSep, @pathList)))
    }



    if( ! open(FILE, ">" . $configSetting{"outFile"} ) ) {
     warn sprintf("Can't write to file %s ",  $configSetting{"outFile"});
     return;
    }
    print FILE join("\n", sort(@records)) ;
    close(FILE);
}

sub getUpdatedHash() {
    return -1 if(@_ == 0) ;

    my $fullPath ;

    $fullPath = File::Spec->rel2abs(shift);
    my @stat = stat($fullPath) ;

    if(! exists($hist{$fullPath})) {
     $hist{$fullPath} = {} ;
     $hist{$fullPath}{"MTIME"} =  $stat[9];
     $hist{$fullPath}{"size"} =  $stat[7];
     $hist{$fullPath}{"md5hash"} = &getMD5Hash($fullPath);
    } elsif(($hist{$fullPath}{"MTIME"} == $stat[9] ) && ($hist{$fullPath}{"size"} == $stat[7] ) )  {
         # No need to change anything. latest update is fine.
    } else {
         $hist{$fullPath}{"md5hash"} = &getMD5Hash($fullPath);
         $hist{$fullPath}{"MTIME"} =  $stat[9];
         $hist{$fullPath}{"size"} =  $stat[7];
    }
    $hist{$fullPath}{"md5hash"} ;
}

sub getMD5Hash() {
    return -1 if(@_ == 0)  ;

    open(FILE, shift) or return -1;
    binmode(FILE);
    my $md5hash = Digest::MD5->new->addfile(*FILE)->hexdigest ;
    close(FILE);
    $md5hash ;
}

sub updateHistoryFile() {
    local $" = ", ";
    my $recSep = $configSetting{"recSep"};
    my @histRecords = ();


    open(HISTFILE, ">" . $configSetting{"historyFile"}) || return ;
    foreach my $fullPath (keys %hist) {
     push(@histRecords, join($recSep, $hist{$fullPath}{"md5hash"}, $hist{$fullPath}{"size"}, $hist{$fullPath}{"MTIME"}, $fullPath));
    }
    print HISTFILE join("\n", sort(@histRecords)) . "\n" ;
    close(HISTFILE);
}

sub check_file {
    -f && ((stat(_))[7] >  $configSetting{"maxSize"} ) && push @{$files{(stat(_))[7]}}, $File::Find::name;
}

__END__;

#print "Processing line : #$_#  \n";
#print "Now processing @{$files{$size}} \n";
