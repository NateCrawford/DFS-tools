#!/bin/bash
# This scriptlet will check the md5sums of tarfiles and non-tarred files transferred with /beegfs/TOOLS/Tar2DFS.sh

#Set pipline return value no last nonzero in pipe
set -o pipefail

usage () {
  echo "Usage: CheckDFS.sh destination_directory"
}

#needs GNU parallel
which parallel  > /dev/null
if [[ $? != 0 ]]; then
  echo "Needs GNU parallel installed"
  exit 3
fi

#some parameters
notarmd5s=RSYNCED.md5sums
TOKfile=TAR2DFS.OK
ChOKfile=CheckDFS.OK
ChNotOKfile=CheckDFS.NOT_OK
Logfile=CheckDFS.log
rechks=CHUNKS_TO_RECHECK

#parallel options
maxp=8
blksz=10000

#init counters/timers
chunkcount=0
errcount=0
starttime=$(date +%s)


#export things needed by functions in parallel
export Logfile errcount

loggit () {
  echo "$(date): $1" | tee -a $Logfile
}

#function to check md5s of tar file in parallel streams
#gets .tar filename and temporary file containing md5sums
partarchk () {
  local tarfile="$1"
  local md5file="$2"

  set -o pipefail
  local errnum=0

  cut -d' ' -f3- "$md5file" > "${md5file}.names"
  local goodmd5=$(cut -d' ' -f1 "$md5file" | cut -d' ' -f1 | md5sum | cut -d' ' -f1) 
  local testmd5=$(tar -xf "$tarfile" --no-wildcards --files-from="${md5file}.names" --to-command "md5sum" | cut -d' ' -f1 | md5sum | cut -d' ' -f1)
  if [[ $? != 0 ]]; then
    loggit "!!!!! Nonzero return value for (tar -xf $tarfile) pipe"
    loggit "$testmd5"
    loggit "Attempting to retry once"
    testmd5=$(tar -xf "$tarfile" --no-wildcards --files-from="${md5file}.names" --to-command "md5sum" | cut -d' ' -f1 | md5sum | cut -d' ' -f1)
  fi
  if [[ $testmd5 != $goodmd5 ]]; then
    local Fline=''
    local gsum=''
    local fil=''
    local tsum=''
    loggit "Some md5sum in $tarfile did not match, need to identify specific errors"
    #Loop over each file individually. Should be in cache, so not big deal?
    while IFS='' read -r Fline || [[ -n "$Fline" ]]; do
      gsum=$(cut -d' ' -f1 <<< $Fline)
      fil="$(cut -d' ' -f2- <<< $Fline)"
      tsum=$(tar -xf "$tarfile" --no-wildcards "$fil" --to-command "md5sum" | cut -d' ' -f1)
      if [[ -z "$tsum" ]]; then
        loggit "ALERT! File $fil appears to be missing from $tarfile"
        ((errnum+=1))
      elif [[ $gsum != $tsum ]]; then
        loggit "ALERT! File $fil has md5sum $tsum in $tarfile, but should have: $gsum"
        ((errnum+=1))
      fi
    done < "$md5file"
  fi
  rm -f "${md5file}.names"
  return $errnum
}
export -f loggit partarchk

if [[ -z $1 ]]; then
  loggit "No directory given for examination"
  usage
  exit 1
elif [[ ! -d $1 ]]; then
  loggit "This thing ($1) is not a directory"
  usage
  exit 2
else
  destdir=$1
fi

cd $destdir
#Check for TOKfile
if [[ ! -f $TOKfile ]]; then
  loggit "Cannot find $TOKfile, the thing that gets written on successful transfer by Tar2DFS.sh."
  loggit "To override, \"touch $TOKfile\" and try again"
  exit 3
fi

#Check for Chunks that previously failed check, skip others
if [[ -f $rechks ]]; then
  loggit "Found $rechks, only re-checking those chunks."
  Tmd5list="$(<$rechks)"
  mv $rechks ${rechks}.old
else
  loggit "Checking all '.chunk_n.tar' files"
  Tmd5list="$(find . -maxdepth 1 -mindepth 1 -name '*.chunk_*.md5' | sed "s:^./::")"
fi

totchunks=$(wc -w <<< $Tmd5list)

#Tar files that were originally parts directories before transfer will have dirname.chunk_#.md5 files
IFS=$'\n'
for Tmd5 in $Tmd5list; do
  Ttar="${Tmd5%%.md5}.tar"
  ((chunkcount+=1))
  loggit "*********** Checking $Ttar ($chunkcount of $totchunks)"
  if [[ ! -f "$Ttar" ]]; then
    loggit "File $Ttar does not exist, but $Tmd5 does. Skipping faulty file"
    ((errcount+=1))
    continue
  fi
  loggit "Roughly $(( ($(stat --format=%s "$Tmd5")/$blksz) + 1 )) jobs"
  cat "$Tmd5" | parallel --halt 2 --progress --pipe --cat --block $blksz --max-procs $maxp partarchk \"$Ttar\" {}
  retval=$?
  if [[ $retval != 0 ]]; then
    ((errcount+=retval))
    loggit "!!!!!!! Errors checking files in $Tmd5. Examine output."
    echo "$Tmd5" >> $rechks 
  else
    loggit "*********** $Ttar OK"
  fi
done
unset IFS

#There should also be a $notarmd5s file that contains md5sums for the rest
loggit "################ Checking $notarmd5s"
loggit "Roughly $(( ($(stat --format=%s $notarmd5s)/$blksz) + 1 )) jobs"
cat $notarmd5s | grep -v $notarmd5s | parallel --progress --pipe --cat --block $blksz --max-procs $maxp md5sum -c --quiet {} | tee -a $Logfile
if [[ $? != 0 ]]; then
  ((errcount+=1))
  loggit "Errors checking files in $notarmd5s. Examine output."
fi

loggit "#################"

if [[ $errcount != 0 ]]; then
  loggit "Encountered at least $errcount errors. Please check."
  echo "CheckDFS.sh finished WITH ERRORS at $(date). Check output!" >> $ChNotOKfile
  chown --reference="$(pwd)" $ChNotOKfile
  rm $ChOKfile
else
  loggit "Directory $(pwd) looks good!"
  echo "The files in $(pwd) match their digests recorded in the .md5 files. CheckDFS.sh finished at $(date)." >> $ChOKfile
  chown --reference="$(pwd)" $ChOKfile
  rm -f $ChNotOKfile $rechks ${rechks}.old
fi

#elapsed time
loggit "Total time elapsed for CheckDFS.sh: $(( ($(date +%s) - starttime)/60 )) minutes"
chown --reference="$(pwd)" $Logfile

exit 0
