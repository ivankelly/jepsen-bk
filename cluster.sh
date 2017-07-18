NODES="n1 n2 n3 n4 n5"

ssh_config() {
    for n in $NODES; do
	IP=$(sudo lxc-attach -n $n -- ip a show dev eth0 | \
		    awk '/inet / { gsub(/\/.*/, "", $2); print $2 }')
	cat <<EOF
Host $n
    user root
    StrictHostKeyChecking no
    Hostname $IP

EOF
    done
}

nodes() {
    for n in $NODES; do
	IP=$(sudo lxc-attach -n $n -- ip a show dev eth0 | \
		    awk '/inet / { gsub(/\/.*/, "", $2); print $2 }')
	echo $IP
    done
}

build() {
    for n in $NODES; do
	sudo lxc-create -n $n --template=debian
	sudo lxc-start -n $n
    done

    for n in $NODES; do
	sudo lxc-attach -n $n -- ip a show dev eth0 | grep -q "inet "
	while [ $? != 0 ]; do
	    echo "$n not up, waiting"
	    sleep 1;
	done

	sudo lxc-attach -n $n -- mkdir /root/.ssh
        cat ~/.ssh/id_rsa.pub | sudo lxc-attach -n $n --\
				     sh -c "cat - > /root/.ssh/authorized_keys"
    done

    sleep 5
    DATE=$(date +%s)
    CONFIG=ssh_config.$DATE
    NODESFILE=nodes.$DATE

    ssh_config > $CONFIG
    echo "SSH config written to $CONFIG"
    rm  ssh_config.latest || true
    ln -s $CONFIG ssh_config.latest

    nodes > $NODESFILE
    echo "Nodes written to $NODESFILE"
    rm nodes.latest || true
    ln -s $NODESFILE nodes.latest
}

kill() {
    for n in $NODES; do
	sudo lxc-stop -n $n
	sudo lxc-destroy -n $n
    done
}

case "$1" in
build)
    build
    ;;
ssh_config)
    ssh_config
    ;;
nodes)
    nodes
    ;;
kill)
    kill
    ;;
*)
    echo "Usage: $0 <build|nodes|ssh_config|kill>"
    exit 1
esac

