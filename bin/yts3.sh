#!/bin/bash

case $0 in
    /*)
        SCRIPT="$0"
        ;;
    *)
        PWD=`pwd`
        SCRIPT="$PWD/$0"
        ;;
esac

CHANGED=true
while [ "X$CHANGED" != "X" ]
do
    # Change spaces to ":" so the tokens can be parsed.
    SAFESCRIPT=`echo $SCRIPT | sed -e 's; ;:;g'`
    # Get the real path to this script, resolving any symbolic links
    TOKENS=`echo $SAFESCRIPT | sed -e 's;/; ;g'`
    REALPATH=
    for C in $TOKENS; do
        # Change any ":" in the token back to a space.
        C=`echo $C | sed -e 's;:; ;g'`
        REALPATH="$REALPATH/$C"
        # If REALPATH is a sym link, resolve it.  Loop for nested links.
        while [ -h "$REALPATH" ] ; do
            LS="`ls -ld "$REALPATH"`"
            LINK="`expr "$LS" : '.*-> \(.*\)$'`"
            if expr "$LINK" : '/.*' > /dev/null; then
                # LINK is absolute.
                REALPATH="$LINK"
            else
                # LINK is relative.
                REALPATH="`dirname "$REALPATH"`""/$LINK"
            fi
        done
    done

    if [ "$REALPATH" = "$SCRIPT" ]
    then
        CHANGED=""
    else
        SCRIPT="$REALPATH"
    fi
done

# Get the location of the script.
REALDIR=`dirname "$REALPATH"`
# Normalize the path
REALDIR=`cd "${REALDIR}/../"; pwd`

export YTFS_HOME=$REALDIR

if [ -z $YTFS_HOME ]; then  
    echo "Environment variable 'YTS3_HOME' not found "
    exit 0;
fi 

echo "YTFS_HOME:$YTFS_HOME"

source $YTFS_HOME/bin/yts3.ev

cd $YTFS_HOME

case "$1" in
start)
    ./YTS3 start  
    ;;
stop)
    ./YTS3 stop
    ;;
console)
    ./YTS3 console
    ;;
restart)
    ./YTS3 restart
    ;;
install)
    ./YTS3 install
    ;;
uninstall)
    ./YTS3 uninstall
    ;;
*)
    echo "usage: $0 console|start|stop|restart|install|uninstall"
    exit 0;
esac
exit
