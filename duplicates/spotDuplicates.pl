#!perl.exe

# ----------------------------------------------------------------------
# 
# Still ToDo:
# 1. Chdir to deeply nested directory fails. Need to fix it!
# 2. Extra set of ||| at end of hst file
# 3. Parallel processing of directories based upon number of processors
#    available
# 4. 
# 
# ----------------------------------------------------------------------

use strict;
use File::Find;
use File::Spec;
use Digest::MD5;
use Data::Dumper;

my %files;
my %md5;
my %hist;
my $THREADS = 12 ;# Number of threads
my %configSetting = (
	"maxSize" => 1.0e6,
	#"maxSize" => 7.0e7,
          "outFile" => "duplicateFiles.list",
          "recSep" => '|',
          "historyFile" => "checksumHistory.hst",
	  "backupWithTimeStamp" => 1,
	  "totalThreads" => 16,
	  "dequeTimeout" => 3,
     );
my @newFilesHashComputation = ();
my $wasted = 0;



print "Processing files ....\n" ;
if(@ARGV) {
	find(\&check_file, @ARGV );
} else {
	find(\&check_file, ".");
}

&loadHistoryFile();
@newFilesHashComputation = &createPossibleDuplicateList();
&computeHashFiles(@newFilesHashComputation);	# Use of parallelization ...
&createDuplicateList();
&updateHistoryFile();
exit 0;

# ------------------------------------------------------------------------------------------------------------------------
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
# Simple working one ...
#
sub computeHashFilesSingleThreaded {
    foreach my $file (@_) {
	if( exists($hist{$file})) {	
	    warn "Some problem in history (duplicate action!) calculation for $file !\n" ;
	    next;
	} 
	$hist{$file} = {} ;
	$hist{$file}{"md5hash"} = &getMD5Hash($file);
	my @stat = stat($file) ;
	$hist{$file}{"MTIME"} =  $stat[9];
	$hist{$file}{"size"} =  $stat[7];
     }
}

#
# Yet to be completed (TODO)
#
sub computeHashFiles {
    my(@newFilesHashComputation) = @_ ;
    &setupThreadEnvAndRun(@newFilesHashComputation);

#     foreach my $file (@newFilesHashComputation) {
# 	if( exists($hist{$file})) {	
# 	    warn "Some problem in history (duplicate action!) calculation for $file !\n" ;
# 	    next;
# 	} 
# 	$hist{$file} = {} ;
# 	$hist{$file}{"md5hash"} = &getMD5Hash($file);
# 	$hist{$file}{"MTIME"} =  $stat[9];
# 	$hist{$file}{"size"} =  $stat[7];
#      }
}

# Update hash checksum for single file 
sub updateHashChecksum {
    my ($string, $file, $hash, $size, $mtime) = @_ ;
    if( !exists($hist{$file})) {	
	$hist{$file} = {} ;
	#warn "Some problem in history (duplicate action!) calculation for $file !\n" ;
	#next;
    } 
    $hist{$file}{"md5hash"} = $hash ;
    my @stat = stat($file) ;
    $hist{$file}{"MTIME"} =  $mtime ;
    $hist{$file}{"size"} =  $size ;
}

sub createPossibleDuplicateList() {
    local $" = ", ";
    print "Figuring out files for which to compute checksum for \n" ;
    our @filesForHashComputations = () ;
    my $totalSize = 0 ;
    foreach my $size (sort {$b <=> $a} keys %files) {
	next unless @{$files{$size}} > 1;
	# Scaling Operation => Can utilize multi processing here !!
	#print "Processing size $size and files: \n\t" . join("\n\t", @{$files{$size}}) . "\n" ;
	foreach my $file (@{$files{$size}}) {
	    my $fullPath = File::Spec->rel2abs($file);
	    my @stat = stat($fullPath) ;	# ToCheck / ToDo : Some optimization possible???
	    if(! -f $fullPath ) {
		    # File doesn't exists ! No point planning for MD5 Hash
		    next ;
	    }
	    if( exists($hist{$fullPath})) {
		if(($hist{$fullPath}{"MTIME"} == $stat[9] ) && ($hist{$fullPath}{"size"} == $stat[7] ) )  {
		    # No need to get updated MD5 Hash
		    next ;
		}
	    }
	    $totalSize += $size ;
	    push(@filesForHashComputations, $fullPath);
	 }
    }
    print "Need to get MD5 Hash for (Total $totalSize bytes): \n" . join("\n", @filesForHashComputations) . "\n" ;
    #print "Need to get MD5 Hash for (Total $totalSize bytes): \n" . join("\n", @filesForHashComputations) . "\n" ;
    @filesForHashComputations;
}

# ------------------------------------------------------------------------------------------------------------------------


sub createDuplicateList() {
    local $" = ", ";
    foreach my $size (sort {$b <=> $a} keys %files) {
     next unless @{$files{$size}} > 1;
     # Scaling Operation => Can utilize multi processing here !!
     foreach my $file (@{$files{$size}}) {
	-f $file || next ;
	#-f $file or next ;
         my $md5hash =$hist{$file}{"md5hash"} ; 
         if( exists($md5{$md5hash})) {
          push @{$md5{$md5hash}{"files"}}, $file;
         } else {
          $md5{$md5hash} = {} ;
          $md5{$md5hash}{"size"} = $hist{$file}{"size"};
          $md5{$md5hash}{"checksum"} = $md5hash;
          push @{$md5{$md5hash}{"files"}},$file;
         }
         #push @{$md5{$md5hash}},$file;
     }
    }
    print Dumper(\%hist);
    print Dumper(\%md5);
    foreach my $hash (keys %md5) {
	next unless @{$md5{$hash}{"files"}} > 1;
	next if ($hash eq "") ;		# Required to handle cases of non-computation of MD5 hash for some files !!!
	my $size = $md5{$hash}{"size"};
	print "$size: " . join(" ", @{$md5{$hash}{"files"}}) . "\n";
	$wasted += $size * (@{$md5{$hash}{"files"}} - 1);
    }
    &dumpFileList();

    1 while $wasted =~ s/^([-+]?\d+)(\d{3})/$1,$2/;
    print "$wasted bytes in duplicated files\n";

}

sub dumpFileList {
    my @records = ();
    my @filesList = ();
    my $recSep = $configSetting{"recSep"};

    foreach my $checkSum (keys %md5) {
	next if ($checkSum eq "") ;	# To avoid blank entries in duplicate files list
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

# sub getUpdatedHash() {
#     return -1 if(@_ == 0) ;
# 
#     my $fullPath ;
# 
#     $fullPath = File::Spec->rel2abs(shift);
#     my @stat = stat($fullPath) ;
# 
#     if(! exists($hist{$fullPath})) {
#      $hist{$fullPath} = {} ;
#      $hist{$fullPath}{"MTIME"} =  $stat[9];
#      $hist{$fullPath}{"size"} =  $stat[7];
#      $hist{$fullPath}{"md5hash"} = &getMD5Hash($fullPath);
#     } elsif(($hist{$fullPath}{"MTIME"} == $stat[9] ) && ($hist{$fullPath}{"size"} == $stat[7] ) )  {
#          # No need to change anything. latest update is fine.
#     } else {
#          $hist{$fullPath}{"md5hash"} = &getMD5Hash($fullPath);
#          $hist{$fullPath}{"MTIME"} =  $stat[9];
#          $hist{$fullPath}{"size"} =  $stat[7];
#     }
#     $hist{$fullPath}{"md5hash"} ;
# }

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
	next if( $hist{$fullPath}{"md5hash"} eq "") ; # Skip case of empty MD5 hash
	push(@histRecords, join($recSep, $hist{$fullPath}{"md5hash"}, $hist{$fullPath}{"size"}, $hist{$fullPath}{"MTIME"}, $fullPath));
    }
    print HISTFILE join("\n", sort(@histRecords)) . "\n" ;
    close(HISTFILE);
}

sub check_file {
    # Creating a hash array mapping of size of file to corresponding file paths!
    -f && ((stat(_))[7] >  $configSetting{"maxSize"} ) && push @{$files{(stat(_))[7]}}, $File::Find::name;
}

# ---------------------------------------------------------------------------------------------------------------------

our $retq;
our $workq ;
sub setupThreadEnvAndRun {
    use threads;
    use Thread::Queue;

    #my $THREADS = $configSettings{"totalThreads"}; # Number of threads
    $retq = Thread::Queue->new(); # Thread return values
				     #(if you care about them)
    $workq = Thread::Queue->new(); # Work to do

print "Queuing up work item @_ \n ";
    $workq->enqueue(@_); # Queue up some work to do
    $workq->enqueue("EXIT") for(1..$THREADS); # And tell them when
					      # they're done

    threads->create("Handle_Work") for(1..$THREADS); # Spawn our workers

    # Process returns while the the threads are running.
    # Alternatively, if we just want to wait until they're all done:
    # sleep 10 while threads->list(threads::running);
    while(threads->list(threads::running)){
      # Blocking dequeue with 5-second timeout
      if (defined(my $data = $retq->dequeue_timed($configSetting{"dequeTimeout"}))) {
	# ----------------------------------------------------------------------
	# Work on $data
	# ----------------------------------------------------------------------
	#print "Results : " . $data->[0] ;
	&updateHashChecksum(@{$data});
      }
    }
    # When we get here, there are no more running threads.
    # At this point we may want to take one more run through the
    # return queue, or do whatever makes sense.

}

sub Handle_Work {
  while(my $todo=$workq->dequeue()) {
    last if $todo eq 'EXIT'; # All done

    # ----------------------------------------------------------------------
    # ...Do work here...
    # ----------------------------------------------------------------------
    ## Process $work to produce $result ##
    my $result;
    my $hash = &getMD5Hash($todo);
    my @stat = stat($todo);
    $result = ["result for workitem $todo ($hash) (@stat )", $todo, $hash, $stat[7], $stat[9] ];
		# Print String, file-path, Hash, Size, Mtime
    # ----------------------------------------------------------------------

    # If that work might generate an error and cause the
    # thread to exit/die prematurely:
    eval {
      # do dangerous work here
    };
    if($@) {
      # if we want to requeue this work to do later
      #(eg. a temporary failure)
      $workq->extract(-1); # Removes the last 'EXIT' from the queue
      $workq->enqueue($todo,"EXIT"); # queue back up this work unit,
                                     # and the 'EXIT' we stripped
      next; # Do the next thing
    } 

    # ...Do more work here, perhaps...

    #$Qresults->enqueue( $result );
    $retq->enqueue($result);
  }

  # We're all done with this thread
  threads->detach;
}


# ----------------------------------------------------------------------------------------------------------------------








__END__;

#print "Processing line : #$_#  \n";
#print "Now processing @{$files{$size}} \n";
