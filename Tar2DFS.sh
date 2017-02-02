#!/bin/bash

#Tar2DFS.sh

#Script to make uncompressed Tar of directories with too many children, and contents/md5sum list, directly
#onto target Distributed File System (/beegfs or /lustre), that doesn't like many tiny files.

#Top-level directories with >= cutoff entries/subentries/subsubentries, etc. will be split into batches and tarred
# without compression (and also md5 digested)

#Top-level files, and top-level Dirs with < cutoff entries, will be transferrred with rsync (and md5 digested) 

#Set pipline return value no last nonzero in pipe
set -o pipefail

usage () {
  echo "Usage: Tar2DFS.sh source_dir dest_dir"
}

#needs GNU parallel
which parallel  > /dev/null
if [[ $? != 0 ]]; then
  echo "Needs GNU parallel installed"
  exit 3
fi


#cutoff for number of files to invoke tar on a directory. (If nfiles > cutoff, tar.)
cutoff=1000
#parallel options
maxp=4 #maximum simultaneous parallel operations
blksz=100000 #rough block size in bytes to split input lists catted to parallel

#specific file names (should agree with checking utility /beegfs/TOOLS/CheckMD5s.sh)
rsyncfail=PARRSYNC_FAILURE_LIST
notarobjs=OBJECTS_TO_TRANSFER_UNTARRED
notarmd5s=RSYNCED.md5sums
OKfile=TAR2DFS.OK
NotOKfile=TAR2DFS.NOT_OK
Logfile=TAR2DFS.log

#init counters/timers
dircount=0
tarcount=0
errcount=0
starttime=$(date +%s)

#export things needed by functions in parallel
export Logfile errcount

loggit () {
  echo "$(date): $1" | tee -a $Logfile
}


#Set up functions to be called by parallel 

#parfind = initial quick find of directory contents.
#  It is called once per main-level directory. IO cost: meta; Net cost: none
parfind () {
  local DIR="$1"
  find "$DIR" -type f -print0 > "${DIR}.dircontents"
}


#partar = tar -cf $dest/$dir.chunk_$N.tar filelist; chown --reference=$src/$dir $dest/dir.tar
# called as: partar 
#  echo "Roughly $(($(stat --format=%s ${DIR}.dircontents)/$blksz)) jobs"
#  cat ${DIR}.dircontents | parallel --halt 2 --progress --pipe --cat --block $blksz -P $maxp partar "$dest" "$DIR" {#} {}
#xargs: unmatched single quote; by default quotes are special to xargs unless you use the -0 option
partar () {
  local DEST="$1"
  local NAME="$2"
  local chnum="$3"
  local filelist="$4"

  set -o pipefail
  local errnum=0

#  #tar and digest the block of files (null terminated filenames)
  tar --create --file="${DEST}/${NAME}.chunk_${chnum}.tar" --null --files-from="$filelist"
  local tarretval=$?
  #Strip out leading "\" that md5sum uses to signal escape sequences in filenames. The CheckDFS.sh script gets the
  #filename directly from the tar file, which produces no leading "\" even for strange names.
  cat "$filelist" | xargs -0 -I '{}' -n 1 md5sum "{}" | sed 's/^\\//' > "${DEST}/${NAME}.chunk_${chnum}.md5"
  local md5retval=$?
  if [[ $tarretval == 0 && $md5retval == 0 ]]; then
    chown --reference="$NAME" "${DEST}/${NAME}.chunk_${chnum}.tar" "${DEST}/${NAME}.chunk_${chnum}.md5"
    #cp "$filelist" "${NAME}.chunk_${chnum}.OK.list"
  else
    cp "$filelist" "${NAME}.chunk_${chnum}.failed.list"
    loggit "Failed to tar/md5 $NAME to $DEST with tar/md5 error codes $tarretval/$md5retval"
    mv "${NAME}.dircontents" "${NAME}.tar_retry"
    ((errnum+=1))
  fi
  return $errnum
}

#parrsync = rsync of dir or file to destination.
#  It is called for each non-tarred object in the main level. IO cost: meta+dataread; Net cost: meta+datawrite
parrsync () {
  local NAME="$1"
  local DEST="$2"
  local errnum=0

  rsync -a "$NAME" "${DEST}"
  local rsyncretval=$?
  if [[ $rsyncretval != 0 ]]; then
    loggit "Failed to rsync $NAME to $DEST with error code $rsyncretval"
    ((errnum+=1))  
  fi
  return $errnum
}

#Must export functions to be able to run with parallel 
export -f loggit parfind partar parrsync

#Test inputs
if [[ -z $1 ]]; then
  echo "No source given"
  usage
  exit 11
elif [[ -z $2 ]]; then
  echo "No destination given"
  usage
  exit 12
fi

srcdir=$1
destdir=$2

if [[ ! -d $srcdir ]]; then
  echo "Source directory $srcdir not a directory. Aborting"
  usage
  exit 1
fi
if [[ ! -d $destdir ]]; then
  echo "Destination directory $destdir not a directory. Aborting"
  usage
  exit 2
fi

cd $srcdir

#Start
loggit "Attempting to Tar/Transfer $srcdir to $destdir"
chown --reference="$destdir" $Logfile

#Check if previously successful
if [[ -f $OKfile ]]; then
  loggit "Found $OKfile:"
  loggit $(<$OKfile)
  loggit "if you want to double-check for newer files, try this:"
  loggit "find $srcdir -type f -print0 | xargs -0 stat --format '%Y :%y %n' | sort -nr | cut -d: -f2- | head -n 2"
  loggit "If it returns $Logfile and $OKfile, nothing is newer."
  exit 0
fi


# quick check for existing .dircontents
if [[ $(find . -mindepth 1 -maxdepth 1 -name '*.dircontents' | wc -l) != 0 ]]; then
  loggit "Existing .dircontents files, abort!"
  exit 4
elif [[ $(find . -mindepth 1 -maxdepth 1 -name '*.tar_retry' | wc -l) != 0 ]]; then
  #only re-tar failed directories
  loggit ".tar_retry files found, attempting re-tar and skipping other objects"
  rename .tar_retry .dircontents *.tar_retry
  totdirs=$(find -mindepth 1 -maxdepth 1 -type f -name '*.dircontents' | wc -l)
elif [[ -f $rsyncfail ]]; then
  #check for failure of final rsync
  loggit "rsync failed previously, but tar OK, only redoing rsync"
  loggit "previously failed transfers:"
  cat $rsyncfail
  rm $rsyncfail
else
  #no failed previous transfers, regenerate .dircontents files for all
  loggit "Creating directory contents lists"
  #Parallelized over directories, reasonably fast, produces ${DIR}.dircontents
  find -mindepth 1 -maxdepth 1 -type d -print0 | parallel -0 -n 1 -P $maxp --bar parfind "{/}"
  totdirs=$(find -mindepth 1 -maxdepth 1 -type d | wc -l)
fi

#mark only those with more than cutoff entries
echo -n > $notarobjs
echo -n > $notarmd5s
IFS=$'\n'
for Clist in $(find -mindepth 1 -maxdepth 1 -type f -name "*.dircontents" -printf '%P\n'); do
#  nentries=$(wc -l "$Clist" | cut -d' ' -f1)
  nentries=$(grep -cz ".*" "$Clist")
  if [[ -z "$nentries" ]]; then
    loggit "!!!! Error, cannot count number of entries in list ($Clist)! Skipping."
    ((errcount+=1))
    continue
  fi
  Cdir="${Clist%%.dircontents}"
  ((dircount+=1))
  loggit "************* Examining $Cdir ($dircount of $totdirs)"
  loggit "$Cdir has $nentries entries"
  if [[ $nentries -gt $cutoff ]]; then
    loggit "Directory $Cdir is being archived."
    loggit "Roughly $(( ($(stat --format=%s "$Clist")/$blksz) + 1 )) jobs"
    cat "$Clist" | parallel --null --halt 2 --progress --pipe --cat --recend '\0' --block $blksz -P $maxp partar \"$destdir\" \"$Cdir\" "{#}" {}
    retval=$?
    if [[ $retval != 0 ]]; then
      loggit "!!!!! Error with partar $destdir $Cdir something something"
      ((errcount+=retval))
    fi
    rm "$Clist" 
  else
    loggit "Directory $Cdir is being md5sum digested to $notarmd5s."
    #Don't strip out leading "\"; it is necessary to signal escape sequences in non-tarred files.
    cat "$Clist" | parallel --null --halt 2 --bar -P $maxp md5sum {}  >> $notarmd5s
    retval=$?
    if [[ $retval != 0 ]]; then
      loggit "!!!!! Error with parallel md5sum of $Cdir"
      ((errcount+=retval))
    fi
    rm "$Clist"
    echo "$Cdir" >> $notarobjs
  fi
done
unset IFS

loggit

#Transfer untarred Objects
loggit "################ Rsyncing everything else"
#add files
find -mindepth 1 -maxdepth 1 -type f ! -name $notarobjs -printf '%P\n' >> $notarobjs
#append md5s of files to $notarmd5s
find -mindepth 1 -maxdepth 1 -type f ! -name '*.dircontents' -a ! -name $notarobjs -a ! -name $notarmd5s -a ! -name $OKfile -exec md5sum '{}' \; >> $notarmd5s
chown --reference="$destdir" $notarmd5s
#parallel rsync
cat $notarobjs | parallel -n 1 -P $maxp --bar parrsync "{}" "$destdir/"

loggit "################"
#cleanup
rm $notarobjs $notarmd5s
find -mindepth 1 -maxdepth 1 -type f -name '*.dircontents' -delete

#Set OK/Check if OK
if [[ $(find . -mindepth 1 -maxdepth 1 -name '*.tar_retry' | wc -l) != 0 ]]; then
  loggit "Some large directories were not tarred properly, check:"
  find . -mindepth 1 -maxdepth 1 -name '*.tar_retry' | tee -a $Logfile
elif [[ $errcount != 0 ]]; then
  loggit "Total $errcount errors, check $srcdir/$Logfile"
else
  loggit "Transfer/Tar from $(pwd) to $destdir looks OK"
  echo "This directory was copied to $destdir with Tar2DFS.sh at $(date)." > $OKfile
  echo "This directory was copied from $(pwd) with Tar2DFS.sh at $(date)." > "$destdir"/$OKfile
  chown --reference="$destdir" $OKfile "$destdir"/$OKfile
fi

#elapsed time
loggit "Total time elapsed for Tar2DFS.sh: $(( ($(date +%s) - starttime)/60 )) minutes"



exit 0
