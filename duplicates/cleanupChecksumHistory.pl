#!perl.exe

use strict;

my %hist;
my %configSetting = (
	"maxSize" => 5.0e6,
	#"maxSize" => 7.0e7,
          "outFile" => "duplicateFiles.list",
          "recSep" => '|',
          "historyFile" => "checksumHistory.hst",
	  "backupWithTimeStamp" => 1,
     );



print "Processing cleanup history command ....\n" ;
&loadHistoryFile();
&cleanupHistoryFile();
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

sub cleanupHistoryFile()  {
	foreach my $fullPath (keys %hist) {
		if( ! -e $hist{$fullPath} ) {
			delete($hist{$fullPath}) ;
		}
	}
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

__END__;

