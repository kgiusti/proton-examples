# simple make file for the proton clients
# Must have the proton development packages installed
# (e.g. via epel: yum install qpid-proton-c-devel
# (or 'add-apt-repository ppa:qpid/released
#      apt-get update
#      apt-get install libqpid-protonX-dev

all: proton-sender proton-receiver

proton-sender: proton-sender.c proton-common.c
	gcc -std=gnu99 -Wall -g -Os -fno-omit-frame-pointer -lqpid-proton -lm -o $@ $^

proton-receiver: proton-receiver.c proton-common.c
	gcc -std=gnu99 -Wall -g -Os -fno-omit-frame-pointer -lqpid-proton -lm -o $@ $^


.PHONY: clean
clean:
	rm -f *.o proton-receiver proton-sender

