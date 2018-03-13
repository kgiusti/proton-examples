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
#include <time.h>

#include "proton-common.h"

#define ROW_START()        do {} while (0)

#define COL_HDR(NAME)      printf("| %20.20s", (NAME))
#define COL_ID(NAME,VAL) printf("| %20ld",(VAL))
#define COL_INT(NAME,VAL)  printf("| %20d", (VAL))
#define COL_TIME(NAME,VAL) printf("| %20ld ", (VAL))
#define COL_CLOCK(NAME,VALS,VALD) printf("| %20s.%ld",(VALS),(VALD))
#define COL_STR(NAME,VAL)  printf("| %20s ", (VAL))
#define ROW_END()     do {                 \
                                printf("\n");   \
                                rows_written++; \
                        } while (0)


static int outlier_threshold = 10;  // msec
static bool bad_clock = false;
static int done = 0;
static void
stop (int sig)
{
  done = 1;
}

// return wall clock time not in msec but in usec
static time_t now ()
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
typedef struct
{
  int debug;
  hosts_t host_addresses;
  int message_count;
  int pre_fetch;
  const char *target;
  uint32_t display_interval_sec;
  int latency;
  long int last_then; 
  int dump_csv;
  char *decode_buffer;
  size_t buffer_len;
  pn_message_t *message;
  uint64_t expected_sequence;
  unsigned received_msgs;
  unsigned dropped_msgs;      // based on sequence
  unsigned duplicate_msgs;    // based on sequence
  unsigned old_msgs;          // sent before receiver started (ignored)
  unsigned future_msgs;       // sent time > now, likely clock inaccurate
  unsigned no_time_msgs;      // message did not contain a timestamp

  time_t start;
  time_t first_msg;
  unsigned long received_count;
  // these are all in msec
  float max_latency;
  float min_latency;
  float total_latency;
  /* distribution in msec over 0..9, 10-99, 100-999, ... 99999 */
#define MAX_ORDER 4
  unsigned distribution[MAX_ORDER][100];
  unsigned overflow;
  unsigned reconnects;
  unsigned duration;
} app_data_t;

// helper to pull pointer to app_data_t instance out of the pn_handler_t
//
#define GET_APP_DATA(handler) ((app_data_t *)pn_handler_mem(handler))

// Called when reactor exits to clean up app_data
//
static void delete_handler (pn_handler_t * handler)
{
  app_data_t *app_data = GET_APP_DATA (handler);
  if (app_data->message)
    {
      pn_decref (app_data->message);
      app_data->message = NULL;
    }

  free (app_data->decode_buffer);
}

static void formatLocaltime (unsigned long long  _time)
{
   time_t l_time;
   l_time=_time/1000;
   char *timestamp = malloc (sizeof (char) * 64);
   strftime(timestamp , 64, "%c", localtime(&l_time));
  //strftime(timestamp , 64, "%Y:%m:%d %H:%M:%S", localtime(&l_time));
   printf("| %20s.%llu",timestamp,_time%1000);

  
}

static void print_latency (app_data_t * data, time_t msecs, time_t then, time_t now,long id)
{
  static int rows_written = 0;
  long int pause_time=0;
  if(data->last_then)
     pause_time=then-data->last_then;
  data->last_then=then;

  // avoid displaying anything if the latency is in the acceptable range
  if (msecs < outlier_threshold)
    {
      return;
    }

  if (!rows_written)
    {
      ROW_START ();
      COL_HDR ("THEN DATE");
      COL_HDR ("NOW DATA");
      COL_HDR("ID");
      COL_HDR ("COUNT");
      COL_HDR ("THEN");
      COL_HDR ("NOW");
      COL_HDR("PAUSE_TIME");
      COL_HDR ("LATENCY");
      ROW_END ();

    }

  char buffer[1024];
  size_t buffsize = sizeof (buffer);
  pn_data_t *body = pn_message_body (data->message);
  pn_data_format (body, buffer, &buffsize);
  ROW_START ();
  formatLocaltime(then);
  formatLocaltime(now);
  COL_ID("id",id);
  COL_INT ("count", rows_written);
  COL_TIME ("then_msecs", then);
  COL_TIME ("now_msecs", now);
  COL_TIME ("pause_time",pause_time);
  COL_TIME ("latency", msecs);
  ROW_END ();

}

static void update_latency (app_data_t * data, time_t msecs)
{
  if (data->debug)
    fprintf (stdout, "latency %ld\n", msecs);

  if (msecs > data->max_latency)
    data->max_latency = msecs;
  if (data->min_latency == 0 || msecs < data->min_latency)
    data->min_latency = msecs;
  data->total_latency += msecs;

  if (msecs < 100)
    {
      data->distribution[0][msecs]++;
    }
  else if (msecs < 1000)
    {
      data->distribution[1][msecs / 10]++;
    }
  else if (msecs < 10000)
    {
      data->distribution[2][msecs / 100]++;
    }
  else if (msecs < 10000)
    {
      data->distribution[3][msecs / 1000]++;
    }
  else
    {
      data->overflow++;
    }
}


static void display_latency (app_data_t * data)
{
  static unsigned long last_count = 0;

  if (data->received_count == 0)
    return;

  if (data->dump_csv)
    {
      printf ("Messages, Latency (msec)\n");
    }
  else
    {
      printf ("\n\nLatency:   (%lu msgs received", data->received_count);
      if (data->display_interval_sec && data->received_count > last_count)
	printf (", %lu msgs/sec)\n",
		(data->received_count -
		 last_count) / data->display_interval_sec);
      else
	printf (")\n");

      printf ("  Average: %f msec\n"
	      "  Minimum: %f msec\n"
	      "  Maximum: %f msec\n",
	      (data->received_count)
	      ? (data->total_latency / data->received_count)
	      : 0, data->min_latency, data->max_latency);
      if (data->reconnects)
          printf("  Reconnect attempts: %u\n", data->reconnects);
      if (data->dropped_msgs)
          printf("  Dropped: %u\n", data->dropped_msgs);
      if (data->duplicate_msgs)
          printf("  Duplicate: %u\n", data->duplicate_msgs);
      if (data->old_msgs)
          printf("  Stale msgs: %u\n", data->old_msgs);
      if (data->future_msgs)
          printf("  Bad Clock msgs: %u\n", data->future_msgs);
      if (data->no_time_msgs)
          printf("  Missing Timestamp msgs: %u\n", data->no_time_msgs);
      printf ("  Distribution:\n");
    }

  unsigned power = 1;
  int order;
  int i;
  for (order = 0; order < MAX_ORDER; ++order)
    {
      for (i = 0; i < 100; ++i)
	{
	  if (data->distribution[order][i] > 0)
	    {
	      if (data->dump_csv)
		{
		  printf ("%u, %u\n",
			  data->distribution[order][i], power * i);
		}
	      else
		{
		  printf ("    msecs: %d  messages: %u\n",
			  power * i, data->distribution[order][i]);
		}
	    }
	}
      power *= 10;
    }

  if (!data->dump_csv && data->overflow > 0)
    printf ("> 100 sec: %u\n", data->overflow);

  last_count = data->received_count;
}


/* handle the received message
 */
static void process_message(app_data_t *data, const time_t now)
{
  pn_atom_t id = pn_message_get_id(data->message);
  const time_t then = pn_message_get_creation_time (data->message);
  const uint64_t recv_seq = id.u.as_long;

 if (data->debug)
   fprintf (stdout, "Message received!\n");

 ++data->received_count;

 if (then)
   {
     if (then < data->start)
       {
         // message is old - sent before the link came up.  Likely buffered in
         // qdrouterd before receiver started
         ++data->old_msgs;
       }
     else if (now < then)
       {
         // likely clocks are not synchronized between sender and receiver
         ++data->future_msgs;
         if (!bad_clock)
           {
             bad_clock = true;
             fprintf(stderr, "Received timestamp < current time -"
                     " likely client clocks are NOT SYNCHRONIZED!\n");
           }
       }
     else
       {
         print_latency (data, now - then, then, now, recv_seq);
         update_latency (data, now - then);

         // recv_seq == 0 indicates a restart of the sender (not an error)
         if (recv_seq != 0 && recv_seq != data->expected_sequence)
           {
             if (data->debug)
               fprintf(stdout, "Sequence mismatch! Expected %lu, got %lu\n",
                       data->expected_sequence, recv_seq);

             if (recv_seq > data->expected_sequence)
               {
                 data->dropped_msgs += recv_seq - data->expected_sequence;
               }
             else  // older sequence #, likely re-transmit
               {
                 ++data->duplicate_msgs;
               }
           }
         data->expected_sequence = recv_seq + 1;
       }
   }
 else  //  missing timestamp
   {
     if (data->debug)
       fprintf (stdout, "dropping message - no timestamp \n");
     ++data->no_time_msgs;
   }
}

/* Process interesting events posted by the reactor.
 * This is called from pn_reactor_process()
 */
static void event_handler (pn_handler_t * handler,
	       pn_event_t * event, pn_event_type_t type)
{
  app_data_t *data = GET_APP_DATA (handler);

  //if (data->debug)
  //    fprintf(stdout, "Event received: %s\n", pn_event_type_name(type));

  switch (type)
    {

    case PN_CONNECTION_INIT:
      {
	// reactor is ready, create a link to the broker
	pn_connection_t *conn;
	pn_session_t *ssn;
        pn_link_t *receiver;

	conn = pn_event_connection (event);
	pn_connection_open (conn);
	ssn = pn_session (conn);
	pn_session_open (ssn);
	receiver = pn_receiver (ssn, "MyReceiver");
	pn_terminus_set_address (pn_link_source (receiver),
				 data->target);
	pn_link_open (receiver);
	// cannot receive without granting credit:
	pn_link_flow (receiver, data->pre_fetch);
      }
      break;

    case PN_CONNECTION_UNBOUND:
        pn_connection_release(pn_event_connection(event));
        break;

    case PN_LINK_REMOTE_OPEN:
      {
        // discard any messages generated before the link becomes active
        data->start = now();
        data->expected_sequence = 0;    // set by first incoming msg
        bad_clock = false;
      }
      break;

    case PN_LINK_REMOTE_CLOSE:
      {
	// shutdown - clean up connection and session
	// This will cause the main loop to eventually exit
	pn_session_close (pn_event_session (event));
	pn_connection_close (pn_event_connection (event));
      }
      break;

    case PN_DELIVERY:
      {
	// A message has been received
	pn_delivery_t *dlv = pn_event_delivery (event);
        pn_link_t *receiver = pn_event_link (event);
	if (pn_delivery_readable (dlv) && !pn_delivery_partial (dlv))
	  {
	    // A full message has arrived
            ssize_t len;
            time_t _now = now ();
            if (!data->first_msg) data->first_msg = _now;

	    if (data->latency)
	      {
		// try to decode the message body
		if (pn_delivery_pending (dlv) > data->buffer_len)
		  {
		    data->buffer_len = pn_delivery_pending (dlv);
		    free (data->decode_buffer);
		    data->decode_buffer = (char *) malloc (data->buffer_len);
		    if (!data->decode_buffer)
		      fatal ("cannot allocate buffer");
		  }

		// read in the raw data
		len =
		  pn_link_recv (receiver, data->decode_buffer,
				data->buffer_len);
		if (len > 0)
		  {
		    // decode it into a proton message
		    pn_message_clear (data->message);
		    if (PN_OK ==
			pn_message_decode (data->message, data->decode_buffer,
					   len))
                      {
                        process_message (data, _now);
                      }
		  }
	      } else if (0) {
		// try to decode the message body
		if (pn_delivery_pending (dlv) > data->buffer_len)
		  {
		    data->buffer_len = pn_delivery_pending (dlv);
		    free (data->decode_buffer);
		    data->decode_buffer = (char *) malloc (data->buffer_len);
		    if (!data->decode_buffer)
		      fatal ("cannot allocate buffer");
		  }

		// read in the raw data
		len =
		  pn_link_recv (receiver, data->decode_buffer,
				data->buffer_len);
		if (len > 0)
		  {
		    // decode it into a proton message
		    pn_message_clear (data->message);
		    if (PN_OK ==
			pn_message_decode (data->message, data->decode_buffer,
					   len))
                      {
                          process_message (data, _now);
                      }
		  }
	    }

	    if (!pn_delivery_settled (dlv))
	      {
		// remote has not settled, so it is tracking the delivery.  Ack
		// it.
		pn_delivery_update (dlv, PN_ACCEPTED);
	      }

	    // done with the delivery, move to the next and free it
	    pn_link_advance (receiver);
	    pn_delivery_settle (dlv);	// dlv is now freed
	    data->received_msgs++;

	    if (data->message_count > 0 && --data->message_count == 0)
	      {
		// done receiving, close the endpoints
		pn_link_close (receiver);
	      }
            else
              {
                // replenish credit if it drops below 1/2 prefetch level
                int credit = pn_link_credit (receiver);
                if (credit < data->pre_fetch / 2)
                    pn_link_flow (receiver, data->pre_fetch - credit);
              }
	  }
      }
      break;

    case PN_TRANSPORT_ERROR:
      {
	// The connection to the peer failed.
	pn_transport_t *tport = pn_event_transport (event);
	pn_condition_t *cond = pn_transport_condition (tport);
	fprintf (stderr, "Network transport failed!\n");
	if (pn_condition_is_set (cond))
	  {
	    const char *name = pn_condition_get_name (cond);
	    const char *desc = pn_condition_get_description (cond);
	    fprintf (stderr, "    Error: %s  Description: %s\n",
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

static void usage (const char *name)
{
  printf ("Usage: %s <options>\n", name);
  printf ("-a \tThe host address [localhost:5672]\n");
  printf ("-c \t# of messages to receive (-1==forever) [1]\n");
  printf ("-t \tTopic address [topic]\n");
  printf ("-i \tDisplay interval [0]\n");
  printf ("-v \tIncrease debug verbosity\n");
  printf ("-l \tEnable latency measurement\n");
  printf ("-u \tOutput in CSV format\n");
  printf ("-p \tpre-fetch window size [100]\n");
}


/* parse command line options */
static int parse_args (int argc, char *argv[], app_data_t * app)
{
  int c;
  char default_host[15] = "localhost:5672";

  // set defaults:
  app->debug = 0;
  hosts_init(&app->host_addresses, default_host);
  app->message_count = 1;
  app->pre_fetch = 100;
  app->target = "topic";
  app->display_interval_sec = 0;
  app->latency = 0;

  opterr = 0;
  while ((c = getopt (argc, argv, "a:c:t:i:p:S:luvd:")) != -1)
    {
      switch (c)
	{
	case 'h':
	  usage (argv[0]);
	  return -1;
	case 'a':
          hosts_init(&app->host_addresses, optarg);
	  break;
	case 'c':
	  app->message_count = atoi (optarg);
	  break;
	case 't':
	  app->target = optarg;
	  break;
	case 'i':
	  app->display_interval_sec = atoi (optarg);
	  break;
	case 'v':
	  app->debug++;
	  break;
	case 'l':
	  app->latency = 1;
	  break;
	case 'p':
	  app->pre_fetch = atoi (optarg);
	  break;
	case 'u':
	  app->dump_csv = 1;
	  break;
	case 'd':
	  app->duration = atoi (optarg);
	  break;
	default:
	  fprintf (stderr, "Unknown option: %c\n", c);
	  usage (argv[0]);
	  return -1;
	}
    }

  if (app->pre_fetch <= 0)
    fatal ("pre-fetch must be >= zero");

  if (app->display_interval_sec && app->latency == 0)
    fatal ("must enable latency if display enabled");

  if (app->debug)
    {
      fprintf (stdout, "Configuration:\n"
	       " Count: %d\n"
	       " Topic: %s\n"
	       " Display Intrv: %d\n"
	       " Latency: %s\n"
	       " Pre-fetch: %d\n",
	       app->message_count, app->target,
	       app->display_interval_sec,
	       (app->latency) ? "enabled" : "disabled", app->pre_fetch);
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


// create a connection to the server
static void connect (app_data_t *data, pn_reactor_t *reactor, pn_handler_t *handler)
{
  pn_connection_t *conn = NULL;
  const char *host = hosts_get (&data->host_addresses);
  pn_url_t *url = pn_url_parse (host);

  if (url == NULL)
    {
      fprintf (stderr, "Invalid host address %s\n", host);
      exit (1);
    }

  if (data->debug) fprintf(stdout, "Connecting to %s...\n", host);
  conn = pn_reactor_connection_to_host (reactor,
                                        pn_url_get_host (url),
                                        pn_url_get_port (url),
                                        handler);
  pn_decref (url);

  // the container name should be unique for each client
  // attached to the broker
  {
    char hname[HOST_NAME_MAX + 1] = "<unknown host>";
    char cname[256];

    gethostname (hname, sizeof (hname));
    snprintf (cname, sizeof (cname), "receiver-container-%s-%d-%d",
	      hname, getpid (), rand ());

    pn_connection_set_container (conn, cname);
  }
}


int main (int argc, char *argv[])
{
  errno = 0;
  signal (SIGINT, stop);
  signal (SIGALRM, stop);

  /* Create a handler for the connection's events.  event_handler() will be
   * called for each event.  The handler will keep a pointer to the app_data
   * instance which can be accessed when the event_handler is called.
   */
  pn_handler_t *handler = pn_handler_new (event_handler,
                                          sizeof (app_data_t),
					  delete_handler);

  /* set up the application data with defaults */
  app_data_t *app_data = GET_APP_DATA (handler);
  memset (app_data, 0, sizeof (app_data_t));
  app_data->buffer_len = 64;
  app_data->decode_buffer = malloc (app_data->buffer_len);
  if (!app_data->decode_buffer)
    fatal ("Cannot allocate encode buffer");
  app_data->message = pn_message ();
  if (!app_data->message)
    fatal ("Message allocation failed");
  if (parse_args (argc, argv, app_data) != 0)
    exit (-1);

  /* Attach the pn_handshaker() handler.  This handler deals with endpoint
   * events from the peer so we don't have to.
   */
  {
    pn_handler_t *handshaker = pn_handshaker ();
    pn_handler_add (handler, handshaker);
    pn_decref (handshaker);
  }

  if (app_data->duration != 0) {
    alarm(app_data->duration);
  }
    
  while (!done && app_data->message_count != 0)
    {
      time_t last_display = now ();
      time_t display_interval = app_data->display_interval_sec * 1000;
      pn_reactor_t *reactor = pn_reactor ();

      // make pn_reactor_process() wake up every second
      pn_reactor_set_timeout (reactor, 1000);
      pn_reactor_start (reactor);

      connect(app_data, reactor, handler);

      // pn_reactor_process() returns 'true' until the connection is shut down.
      while (!done && pn_reactor_process (reactor))
        {
          if (display_interval)
            {
              time_t _now = now ();
              if (_now >= last_display + display_interval)
                {
                  last_display = _now;
                  display_latency (app_data);
                }
            }
        }
      pn_reactor_free (reactor);

      // if the connection closed, but we're still not done, try to reconnect
      if (!done && app_data->message_count != 0)
        {
          sleep (1);  // prevent busy loop
          app_data->reconnects++;
        }
    }

  if (app_data->latency)
    {
      display_latency (app_data);
    }

  float duration = (now() - app_data->first_msg)/1000.0;
  printf("Recvr Done! messages:%d dropped:%d duration:%f rate:%f\n",
	 app_data->received_msgs,
	 app_data->dropped_msgs,
         duration,
         duration ? (app_data->received_msgs + app_data->dropped_msgs)/duration : -1.0);
  pn_decref (handler);

  return 0;
}
