#!/bin/bash

if [ "$#" -lt 3 ]; then
    echo "Usage: bench.sh [queries] [data] [db]"
    exit
fi


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"
DB="$(basename $3)"

rm -f $2"${DB}"*

for i in {1..5}
do
    echo "Starting: $i"
    printf 'bench %s %s 3\nquit' $1 "$2${DB}-$i.log" | $DIR/skinnerdb $3
    echo "Finished: $i"
done
