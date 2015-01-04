#!d:\utils\perl\bin\perl.exe 
# **********************************************************************
# $Header$
# 
# $Author$
#
# $Log$
#
# MAJOR CHANGES
#   Processes files from duplicatesFiles.list to figure out files
#    to be removed
# **********************************************************************

use Data::Dumper;


my $debugging = 0;
my %md5;
my %configSetting = (
	#"maxSize" => 5.0e6,
	  "maxSize" => 7.0e7,
          "dupLstFile" => "duplicateFiles.list",
          "recSep" => '|',
	  "dirSeparator" => "/",
          "historyFile" => "checksumHistory.hst",
	  "backupWithTimeStamp" => 1,
	  "debug" => 0,
	  "info" => 1,
	  "dryRun" => 0,
	  "patternPriorityForNonDeletion" => ["OneDrive", "data_JUNK", "data_media", "data_to_sort", "2_videos", "1_photos", "0_Inbox", "data_family", "data_media" ],
	  #"patternPriorityForNonDeletion" => ["OneDrive", "2_videos", "1_photos", "0_Inbox", "data_family", "data_media", "data_JUNK" ],
	    # "D:\\data\\data_cloud\\OneDrive\\data_family\\sync_data_family_media\\2_videos",  => Can this also work ??
     );

$oldMediaBaseDir = "d:/data/data_family/sync_data_family_media" ;
&loadDuplicatesList() ;
&markDuplicateFiles();
if( $configSetting{"dryRun"} == 1 ) {
	print "Going to demonstrate which files will be deleted through dry run.\n" ;
	&dryRunRemoveDuplicateFiles();
} else {
	print "WARNGING !!! Will clean up all duplicates leaving only one copy of all duplicate files figuring in list !!\n" ;
	&removeDuplicateFiles();
}
exit 0;

sub loadDuplicatesList() {
    local $" = ", ";
    my $recSep = quotemeta($configSetting{"recSep"});

    print "Processing duplicates list files ....\n" ;
    open(DUPLSTFILE, $configSetting{"dupLstFile"}) || return ;
    $x = 0;
    while(<DUPLSTFILE>) {
	chomp;
	$x++ ;
	my($num, $md5hash, $size, @duplicateFiles ) ;
	($num, $md5hash, $size, @duplicateFiles ) = split($recSep, $_);

	# ------------------------------------------------------------------------
	# HACK for debugging specific records .....
	# MD5hash = 59b5a52ef375b9c0ccc6a443471fedeb
	if( $debugging ) {
		if( $md5hash eq "366eb4083d5a0e8e5c37bfa8fc25a33d" ) {	 # 366eb4083d5a0e8e5c37bfa8fc25a33d , "59b5a52ef375b9c0ccc6a443471fedeb" 
			printf "Found DEBUG POINT $md5hash \n"; # . "for @duplicateFiles (num = $num vs %d)\n", scalar(@duplicateFiles);
		} else {
			next ;
		}
	}
	# ------------------------------------------------------------------------
	
	if(@duplicateFiles != $num) {
	    $dupNum = @duplicateFiles ;
	    warn "Record $x has some problem ($num versus $dupNum, @duplicateFiles ) !!\n" ;
	    next ;
	}
	#print "Hashs $md5 corresponds to following files :\n\t" . join( "\n\t", @duplicateFiles ) . "\n" ;
	$md5{$md5hash} = {};
	$md5{$md5hash}{"md5hash"} = $md5hash ;
	$md5{$md5hash}{"num"} = $num ;
	$md5{$md5hash}{"size"} = $size ;
	$md5{$md5hash}{"files"} = \@duplicateFiles ;
    }
    close(DUPLSTFILE);
}

sub markDuplicateFiles()
{
	my %pattern = ();
        my @allPatternStrs = @{$configSetting{"patternPriorityForNonDeletion"}} ;
    	foreach $md5Hash (keys %md5) {
	    #print "Processing $md5Hash ...\n" ;
	    my @dupFilesList = @{$md5{$md5Hash}{"files"}} ;
	    if($debug) {
		printf "Record (md5:%s) with (num:%d) (size:%d) and files @dupFilesList \n",
		    $md5Hash,
		    $md5{$md5Hash}{"num"},
		    $md5{$md5Hash}{"size"}
		    ;
	    }
	   $num = $md5{$md5Hash}{"num"};
	   $xchk = @dupFilesList ;
	   if($num != $xchk) {
	       warn "Some problem in data structure($md5Hash), skipping !!\n" ;
	       next ;
	   }
	   $pattern{$md5Hash} = {} ; 
	   $md5{$md5Hash}{markForDeletion} = [] ;
	   my $atleastOneFileFound = 0 ;
	   foreach $patternStr (@allPatternStrs) {
	       # Patterns are searched in priority order !!
	        my $found = grep(/$patternStr/, @dupFilesList);	# patternPriorityForNonDeletion
	   	$pattern{$md5Hash}{$patternStr} = $found ;
		if( $found <= 0) {
		    # Pattern not found. Look for next priority pattern
		    next ;
		} elsif( $found == 1 ) {
		    # Exactly one entry found, mark all other entries for deletion !!
		    $md5{$md5Hash}{markForDeletion}[0..$#dupFilesList] = 1 ; # Except one !!
		    for($i = 0 ; $i <= $#dupFilesList ; $i++ ) {
			if( grep(/$patternStr/, $dupFilesList[$i])) {
			   $md5{$md5Hash}{markForDeletion}[$i] = 0 ;
			} else {
			   $md5{$md5Hash}{markForDeletion}[$i] = 1 ;
			}
		    }
		    $atleastOneFileFound = 1;
		    last ;
		} else {
		    # More than one entries found. Mark all other entries + ($found - 1) entries here for deletion !!
		    #$md5{$md5Hash}{markForDeletion}[0..$#dupFilesList] = 1 ; # Except one !!
		    for($i = 0, $found = 0 ; $i <= $#dupFilesList ; $i++ ) {
			if( (!$found) && ($found = grep(/$patternStr/, $dupFilesList[$i])) ) {
			   $md5{$md5Hash}{markForDeletion}[$i] = 0 ;
			   $found++ ;
			} else {
			   $md5{$md5Hash}{markForDeletion}[$i] = 1 ;
			}
		    }
		    $atleastOneFileFound = 1;
		    last ;
		}
	   }
	   if( $atleastOneFileFound == 0 ) {
	       print "No files marked for hash $md5Hash and files @dupFilesList \n" ;
	   }
	}
	#print Dumper(\%md5);
	#print Dumper(\%pattern);
}

sub removeDuplicateFiles()
{
    foreach $md5Hash (keys %md5) {
	my @dupFilesList = @{$md5{$md5Hash}{"files"}} ;
	my @marker = @{$md5{$md5Hash}{"markForDeletion"}} ;
	print "For MD5 ($md5Hash) : Total entries (num = $md5{$md5Hash}{num}) Deleting entries:\n" ;
	for($i = 0 ; $i <= $#marker ; $i++ ) {
	    if(!$marker[$i]) {
		print "\t$dupFilesList[$i] (RETAINING) \n" ;
		next ;
	    }
	    print "Removing $dupFilesList[$i] ... \n" ;
	    if( !unlink($dupFilesList[$i])) {
		  warn "Can't delete file $dupFilesList[$i] \n" ;
	    }
	}
    }
}

sub dryRunRemoveDuplicateFiles()
{
    foreach $md5Hash (keys %md5) {
	my @dupFilesList = @{$md5{$md5Hash}{"files"}} ;
	my @marker = @{$md5{$md5Hash}{"markForDeletion"}} ;
	print "For MD5 ($md5Hash) : Total entries (num = $md5{$md5Hash}{num}) Deletion Entries include :\n" ;
	for($i = 0 ; $i <= $#marker ; $i++ ) {
	    if(!$marker[$i]) {
		print "\t$dupFilesList[$i] (TO RETAIN) \n" ;
		next ;
	    }
	    print "\t$dupFilesList[$i] \n" ;
	}
    }
}

__END__ ;


	#use Cwd ; # 'abs_path' if required	# Not used now
	#use File::Glob  ':globally';	# Not used now
