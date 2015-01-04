#!perl.exe

use strict;

my %hist;
my %configSetting = (
          "outFile" => "duplicateFiles.list",
          "recSep" => '|',
          "historyFile" => "checksumHistory.hst",
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
		if( ! -e $fullPath ) {
			delete($hist{$fullPath}) ;
			#print "Removing $fullPath ...\n" ;
		} else {
			#print "Keeping $fullPath ...\n" ;
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

