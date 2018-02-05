nodes() {
    for n in $(docker ps | awk '/jepsen/ { print $1 }'); do
        docker exec $n ip a show dev eth0 | \
            awk '/inet / { gsub(/\/.*/, "", $2); print $2 }'
    done
}

case "$1" in
nodes)
    nodes
    ;;
*)
    echo "Usage: $0 <nodes>"
    exit 1
esac

