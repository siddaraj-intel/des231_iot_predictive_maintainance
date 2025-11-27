#! /bin/bash

board="brc1"
session="kafka_"$board"_broker"
kafka_home='/home/iisc/kafka_2.13-3.6.2'

function ctrlc_brokers() {
	# STOP - ignore ret code
	set +e
	echo === Stopping brokers ===
	tmux send-keys -t $session:w0.2 C-c
	tmux send-keys -t $session:w0.1 C-c
	tmux send-keys -t $session:w0.0 C-c
	echo === Wait for 2 second ===
	sleep 2
	tmux kill-window -t $session:w0
	tmux kill-session -t $session > /dev/null

	return 0
}

function start_brokers() {
	set -e
	#START
	echo "**** starting new tmux session ****"
	set +e
	systemctl stop firewalld
	set -e
	tmux new-session  -c $kafka_home -d -s $session -n w0
	sleep 1
	tmux split-window -c $kafka_home -h -t $session:w0.0 -p 30
	tmux split-window -c $kafka_home    -t $session:w0.0
	#tmux select-layout -t $session:w0 -p

	sleep 1
	echo === Starting brokers -START ===
	tmux send-keys -t $session:w0.0 "$kafka_home/bin/kafka-server-start.sh /root/$board.properties" ENTER
	echo === Starting brokers - DONE ===

	sleep 5
	echo --- Launching motor_consumer.py START ---
	tmux send-keys -t $session:w0.2 "/tmp/motor_consumer.py" ENTER
	echo --- Launching motor_consumer.py DONE  ---
	set +e

	for cnt in {1..30}; do
		if tmux capture-pane -t $session:w0.0 -pS -5000 | grep -q "Registered broker"; then
			echo "=== SUCCESS: Broker Registered ==="
			return 0
		fi

		sleep 1
	done

	echo "=== ERROR: failed to register broker ==="
	return -1
}

case "$1" in
"start")
	while true; do
		ctrlc_brokers
		start_brokers
		if [ $? -eq 0 ]; then
			break
		fi
	done

	;;
"stop")
	ctrlc_brokers ;;
*)
	echo "ERROR: only start or stop cmds are supported.";
	exit 1;
	;;
esac

