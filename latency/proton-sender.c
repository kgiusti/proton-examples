/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <limits.h>
#include <signal.h>
#include <sys/time.h>
#include <errno.h>

#include "proton/reactor.h"
#include "proton/message.h"
#include "proton/connection.h"
#include "proton/session.h"
#include "proton/link.h"
#include "proton/delivery.h"
#include "proton/event.h"
#include "proton/handlers.h"
#include "proton/transport.h"
#include "proton/url.h"

#include "proton-common.h"

static int done = 0;
static void stop(int sig)
{
    done = 1;
}

// return wall clock time in msec
static time_t now()
{
    struct timeval tv;
    int rc = gettimeofday(&tv, NULL);
    if (rc) fatal("gettimeofday() failed");
    return (tv.tv_sec * 1000) + (time_t)(tv.tv_usec / 1000);
}


// Example application data.  This data will be instantiated in the event
// handler, and is available during event processing.  In this example it
// holds configuration and state information.
//
typedef struct {
    int debug;
    hosts_t host_addresses;
    int send_count;
    const char *target;
    size_t msg_len;
    uint64_t sequence;
    int pre_settle;
    uint32_t pause_min_msec;
    uint32_t pause_max_msec;
    int latency;
    pn_link_t *sender;
    pn_message_t *message;
    char *encode_buffer;
    size_t buffer_len;
    int reconnects;
    int duration;
} app_data_t;

// helper to pull pointer to app_data_t instance out of the pn_handler_t
//
#define GET_APP_DATA(handler) ((app_data_t *)pn_handler_mem(handler))

// Called when reactor exits to clean up app_data
//
static void delete_handler(pn_handler_t *handler)
{
    app_data_t *app_data = GET_APP_DATA(handler);
    if (app_data->message) {
        pn_decref(app_data->message);
        app_data->message = NULL;
    }

    free(app_data->encode_buffer);
}


/* Process interesting events posted by the reactor.
 * This is called from pn_reactor_process()
 */
static void event_handler(pn_handler_t *handler,
                          pn_event_t *event,
                          pn_event_type_t type)
{
    app_data_t *data = GET_APP_DATA(handler);

    //if (data->debug)
    //    fprintf(stdout, "Event received: %s\n", pn_event_type_name(type));

    switch (type) {

    case PN_CONNECTION_INIT: {
        // reactor is ready, create a link to the broker
        pn_connection_t *conn;
        pn_session_t *ssn;

        conn = pn_event_connection(event);
        pn_connection_open(conn);
        ssn = pn_session(conn);
        pn_session_open(ssn);
        data->sender = pn_sender(ssn, "MySender");
        pn_incref(data->sender);
        pn_link_set_snd_settle_mode(data->sender, PN_SND_MIXED);
        pn_terminus_set_address(pn_link_target(data->sender), data->target);
        pn_link_open(data->sender);
    } break;

    case PN_CONNECTION_UNBOUND:
        pn_decref(data->sender);
        data->sender = NULL;
        pn_connection_release(pn_event_connection(event));
        break;

    case PN_LINK_REMOTE_CLOSE: {
        // shutdown - clean up connection and session..
        pn_session_close(pn_event_session(event));
        pn_connection_close(pn_event_connection(event));
    } break;

    case PN_DELIVERY: {
        // This event indicates that our message was (N)ACKed by the peer.
        // These events are only generated when messages are not sent
        // pre-settled
        pn_delivery_t *dlv = pn_event_delivery(event);
        if (pn_delivery_updated(dlv) && pn_delivery_remote_state(dlv)) {
            uint64_t rs = pn_delivery_remote_state(dlv);
            switch (rs) {
            case PN_RECEIVED:
                // This is not a terminal state - it is informational, and the
                // peer is still processing the message.
                break;
            case PN_ACCEPTED:
                // ACK
                pn_delivery_settle(dlv);
                if (data->debug) fprintf(stdout, "Message acknowledged!\n");
                break;
            case PN_REJECTED:
            case PN_RELEASED:
            case PN_MODIFIED:
                // NACK
                pn_delivery_settle(dlv);
                fprintf(stderr, "Message not accepted - %s\n",
                        (rs == PN_REJECTED) ? "REJECTED"
                        : ((rs == PN_RELEASED) ? "RELEASED"
                           : "MODIFIED"));
                // TBD: RELEASED messages should be re-transmitted
                break;
            default:
                // ??? no other terminal states defined, so ignore anything else
                pn_delivery_settle(dlv);
                fprintf(stderr, "Unknown delivery failure - code=%lu\n", (unsigned long)rs);
                break;
            }
        }
    } break;

    case PN_TRANSPORT_ERROR: {
        // The connection to the peer failed.
        pn_transport_t *tport = pn_event_transport(event);
        pn_condition_t *cond = pn_transport_condition(tport);
        fprintf(stderr, "Network transport failed!\n");
        if (pn_condition_is_set(cond)) {
            const char *name = pn_condition_get_name(cond);
            const char *desc = pn_condition_get_description(cond);
            fprintf(stderr, "    Error: %s  Description: %s\n",
                    (name) ? name : "<error name not provided>",
                    (desc) ? desc : "<no description provided>");
        }
        // pn_reactor_process() will exit with a false return value, stopping
        // the main loop.
    } break;

    default:
        // ignore the rest
        break;
    }
}

static void usage(const char *name)
{
    printf("Usage: %s <options>\n", name);
    printf("-a \tThe host address [localhost:5672]\n");
    printf("-c \t# of messages to send (-1==forever) [1]\n");
    printf("-t \tTopic address [topic]\n");
    printf("-s \tMessage size [64]\n");
    printf("-p \tDo not block for ACKs\n");
    printf("-v \tIncrease debug verbosity\n");
    printf("-R \tRandom seed [time]\n");
    printf("-m \tRandom rate minimum (msec)\n");
    printf("-M \tRandom rate maximum (msec)\n");
    printf("-l \tEnable latency measurement\n");
}

/* parse command line options */
static int parse_args(int argc, char *argv[], app_data_t *app)
{
    int c;
    char default_host[15] = "localhost:5672";

    // set defaults:
    srand((unsigned int)now());
    app->debug = 0;
    hosts_init(&app->host_addresses, default_host);
    app->send_count = 1;
    app->target = "topic";
    app->msg_len = 64;
    app->pre_settle = 0;
    app->pause_min_msec = 0;
    app->pause_max_msec = 0;
    app->latency = 0;
    app->duration = 0;

    opterr = 0;
    while((c = getopt(argc, argv, "a:c:t:s:S:R:m:M:lpvd:")) != -1) {
        switch(c) {
        case 'h':
            usage(argv[0]);
            return -1;
        case 'a':
            hosts_init(&app->host_addresses, optarg);
            break;
        case 'c':
            app->send_count = atoi(optarg);
            break;
        case 't': app->target = optarg; break;
        case 's': app->msg_len = atoi(optarg); break;
        case 'p': app->pre_settle = 1; break;
        case 'v': app->debug++; break;
        case 'R': srand(atoi(optarg)); break;
        case 'm': app->pause_min_msec = atoi(optarg); break;
        case 'M': app->pause_max_msec = atoi(optarg); break;
        case 'l': app->latency = 1; break;
        case 'd': app->duration = atoi(optarg); break;
        default:
            fprintf(stderr, "Unknown option: %c\n", c);
            usage(argv[0]);
            return -1;
        }
    }

    if (app->pause_min_msec > app->pause_max_msec)
        fatal("minimum rate must be <= maximum rate");

    if (app->debug) {
        fprintf(stdout, "Configuration:\n"
                " Count: %d\n"
                " Topic: %s\n"
                " Length: %zd\n"
                " Pre-settle: %s\n"
                " Pause (msec): %d min\n"
                "               %d max\n"
                " Timestamp: %s\n",
                app->send_count, app->target,
                app->msg_len, (app->pre_settle) ? "yes" : "no",
                app->pause_min_msec, app->pause_max_msec,
                (app->latency) ? "enabled" : "disabled");
        fprintf (stdout,
                 "  Host(s):");
        for (int i = 0; i < app->host_addresses.count; i++)
            fprintf (stdout,
                     "%s %s",
                     (i == 0) ? "" : ",",
                     app->host_addresses.hosts[i]);
        fprintf (stdout, "\n");
    }

    return 0;
}


// write a message to the link, return 1 if message sent
static int send_message(app_data_t *app_data)
{
    static unsigned long tag = 1;

    size_t msg_len = 0;
    pn_atom_t id = {};
    int rc = 0;

    // be sure we can actually send a message
    if (!app_data->sender
        || pn_link_state(app_data->sender) != (PN_LOCAL_ACTIVE | PN_REMOTE_ACTIVE)
        || pn_link_credit(app_data->sender) <= 0)
        return 0;

    id.type = PN_ULONG;
    id.u.as_ulong = app_data->sequence++;
    pn_message_set_id(app_data->message, id);
    if (app_data->latency) {
        pn_message_set_creation_time(app_data->message,
                                     (pn_timestamp_t)now());
    }

    // now encode the message.
    do {
        msg_len = app_data->buffer_len;
        // note: msg_len is set to the actual encoded data length
        rc = pn_message_encode(app_data->message, app_data->encode_buffer, &msg_len);
        if (rc == PN_OVERFLOW) {
            app_data->buffer_len *= 2;
            free(app_data->encode_buffer);
            app_data->encode_buffer  = (char *)malloc(app_data->buffer_len);
            if (!app_data->encode_buffer) fatal("cannot allocate buffer");
        }
    } while (rc == PN_OVERFLOW);

    {
        pn_delivery_t *delivery;

        delivery = pn_delivery(app_data->sender, pn_dtag((const char *)&tag, sizeof(tag)));
        if (!delivery) fatal("delivery allocation failed");
        ++tag;

        if (app_data->debug) fprintf(stdout, "Sending message...\n");
        pn_link_send(app_data->sender, app_data->encode_buffer, msg_len);
        pn_link_advance(app_data->sender);
        if (app_data->pre_settle) {
            pn_delivery_settle(delivery);
        }
    }
    return 1;
}

static void alarm_handler(int signo) {
  done = 1;
  printf("done! \n");
}

int main(int argc, char *argv[])
{
    errno = 0;
    signal(SIGINT, stop);
    signal(SIGALRM, alarm_handler);

    /* Create a handler for the connection's events.  event_handler() will be
     * called for each event and delete_handler will be called when the
     * connection is released.  The handler will allocate an app_data_t
     * instance which can be accessed when the event_handler is called.
     */
    pn_handler_t *handler = pn_handler_new(event_handler,
                                           sizeof(app_data_t),
                                           delete_handler);

    /* set up the application data with defaults */
    app_data_t *app_data = GET_APP_DATA(handler);
    memset(app_data, 0, sizeof(app_data_t));
    app_data->buffer_len = 64;
    app_data->encode_buffer = malloc(app_data->buffer_len);
    if (!app_data->encode_buffer) fatal("Cannot allocate encode buffer");
    if (parse_args(argc, argv, app_data) != 0) exit(-1);

    /* Attach the pn_handshaker() handler.  This handler deals with endpoint
     * events from the peer so we don't have to.
     */
    {
        pn_handler_t *handshaker = pn_handshaker();
        pn_handler_add(handler, handshaker);
        pn_decref(handshaker);
    }

    // create a single message and pre-build it so we only have to do that
    // once.  All transmits will use the same pre-built message simply for
    // speed.
    //
    app_data->message = pn_message();
    if (!app_data->message) fatal("Message allocation failed");
    pn_message_set_address(app_data->message, app_data->target);
    pn_data_t *body = pn_message_body(app_data->message);
    pn_data_clear(body);

    // This message's body contains a single string
    if (app_data->msg_len > 0) {
        char *msgtext = malloc(app_data->msg_len + 1);
        if (!msgtext) fatal("malloc failed");
        memset(msgtext, 'X', app_data->msg_len);
        msgtext[app_data->msg_len] = 0;
        if (pn_data_fill(body, "S", msgtext))
            fatal("Error building message body");
        free(msgtext);
    }
    pn_data_rewind(body);

    if (app_data->duration != 0) {
      alarm(app_data->duration);
    }
    while (!done && app_data->send_count != 0) {

        pn_reactor_t *reactor = pn_reactor();
        const char *host = hosts_get (&app_data->host_addresses);
        pn_url_t *url = pn_url_parse (host);

        if (url == NULL)
          {
            fprintf (stderr, "Invalid host address %s\n", host);
            exit (1);
          }

        if (app_data->debug)
            fprintf(stdout, "Connecting to %s...\n", host);

        pn_connection_t *conn = pn_reactor_connection_to_host(reactor,
                                                              pn_url_get_host(url),
                                                              pn_url_get_port(url),
                                                              handler);
        if (!conn) fatal("cannot create connection");

        pn_decref(url);

        // the container name should be unique for each client
        // attached to the broker
        {
            char hname[HOST_NAME_MAX+1];
            char cname[256];

            gethostname(hname, sizeof(hname));
            snprintf(cname, sizeof(cname), "sender-container-%s-%d-%d",
                     hname, getpid(), rand());

            pn_connection_set_container(conn, cname);
        }

        // make pn_reactor_process() non-blocking
        pn_reactor_set_timeout(reactor, 0);
        pn_reactor_start(reactor);

        time_t next_transmit = now();
        // pn_reactor_process() returns 'true' until the connection is shut down.
        while (!done && pn_reactor_process(reactor)) {
            time_t n = now();
            if (n >= next_transmit) {
                // pause interval expired, send a message
                if (app_data->send_count && send_message(app_data) > 0) {
                    // message sent, update deadline for next transmit
                    unsigned int pause = 0;
                    if (app_data->pause_max_msec)
                        pause = rand() % app_data->pause_max_msec;
                    if (app_data->pause_min_msec) {
                        pause += app_data->pause_min_msec;
                        pause -= pause % app_data->pause_min_msec;
                    }
                    pn_reactor_set_timeout(reactor, pause);
                    if (app_data->debug)
                        fprintf(stdout, "Random delay: %u msec\n", pause);
                    next_transmit = now() + pause;

                    if (app_data->send_count > 0 && --app_data->send_count == 0) {
                        // this starts the shutdown process
                        pn_link_close(app_data->sender);
                    }
                }
            } else {
                // adjust timeout to account for elapsed time
                pn_reactor_set_timeout(reactor, next_transmit - n);
            }
        }
        pn_reactor_free (reactor);

        // if the connection closed, but we're still not done, try to reconnect
        if (!done && app_data->send_count != 0)
          {
            sleep (1);  // prevent busy loop
            app_data->reconnects++;
          }
    }
    pn_decref (handler);

    return 0;
}
