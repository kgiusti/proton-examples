Example message sender and receiver that measure message latency.

Requires an AMQP 1.0 compatible intermediary, for example the Qpid
Dispatch Router - see:

http://qpid.apache.org/components/dispatch-router/index.html

Be sure to have the required Qpid Proton development libraries.

For RHEL/Centos, enable the EPEL repository and yum install
qpid-proton-c-devel.

For Ubuntu use the Qpid PPA:

   sudo add-apt-repository ppa:qpid/released
   sudo apt-get update
   sudo apt-get install libqpid-proton10-dev

Or pull the sources from Apache and build/install them:

http://qpid.apache.org/proton/index.html

To build: make -f proton.mk

To run the clients (assuming Qpid Dispatch Router is listening on
localhost):

1) start a proton-receiver.  This will start a receiver which will
display the current statistics every 10 seconds (-i 10), measure
latency (-l), and stop after 200 messages (-c 200):

  proton-receiver -a localhost:5672 -i 10 -l -c 200


2) start a proton-sender on another terminal.  This will send 200
messages (-c 200) with latency data contained in the message (-l).
Messages will be transmitted in a somewhat random frequency between
100 and 300 milliseconds (-m 100 -M 300):

  proton-sender -a localhost:5672 -c 200 -l -m 100 -M 300


Note well: the accuracy of the latency is dependent on the sender and
receiver using the same clock.  If the sender and receiver are on
different machines the system clocks must be synchronized to
sub-millisecond accuracy.  Note that the router does not have to be
run on the same machine as the senders/receivers since it does not
factor into the latency timestamps.
