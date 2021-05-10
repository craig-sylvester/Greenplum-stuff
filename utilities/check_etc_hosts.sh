#!/usr/bin/env bash
#############################################################################
# Verify all the host IP addresses across the cluster are unique
# Look for IP addr collisions in the '/etc/hosts' files across the cluster.
#############################################################################

USAGE_MSG="$0 <host file name>"
OUT_FILE=/tmp/$0.out

# File of hosts names is expected:
[[ $# -ne 1  ]] || [[ ! -r $1 ]] && { echo -e "Host list is missing or unreadable.\nUsage: $USAGE_MSG"; exit 1; }
host_file=$1

# Gather all hosts files and write to output (ignore lines with 'localhost' and the command we ran)
gpssh -f $host_file -e 'cat /etc/hosts' | egrep -v 'localhost|cat /etc/hosts' > $OUT_FILE

# The gpssh command outputs the name of the host in the first column inside square brackets.
# And it is right justified to make the first column fixed width. Just remove the space(s) before we make out 'cut' below.
sed -i -e 's/\[ */\[/' $OUT_FILE

# Keep only the first two columns of the file and output the file in sorted order.
cut -d ' ' -f1,2 $OUT_FILE | sort > $OUT_FILE.tmp
mv $OUT_FILE.tmp $OUT_FILE

# Then compare the number of lines in the output file against the number of unique lines in the file.
# If they are different, then an IP address is being used by more than 1 host
cnt_orig=$(wc -l $OUT_FILE | cut -d ' ' -f1)
cnt_sort_uniq=$(sort -u $OUT_FILE | wc -l | cut -d ' ' -f1)

if [[ $cnt_orig -ne $cnt_sort_uniq ]]; then
    echo "IP address collision."
    sort -u $OUT_FILE > $OUT_FILE.uniq
    diff $OUT_FILE $OUT_FILE.uniq
fi
rm -f /tmp/$OUT_FILE.*
