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

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "proton-common.h"


int hosts_init(hosts_t *hosts, char *list)
{
    char *host;

    hosts->count = 0;
    hosts->current = 0;
    while ((host = strtok(list, ", ")) != NULL
           && hosts->count < MAX_HOSTS)
      {
        list = NULL;  // man strtok
        hosts->hosts[hosts->count++] = strdup(host);
      }
    if (host)
        fprintf(stderr, "only 10 failover addresses allowed\n");
    return hosts->count;
}

const char *hosts_get(hosts_t *hosts)
{
    // return current host, advance to next host
    const char *current = hosts->hosts[hosts->current];
    hosts->current = (hosts->current + 1) % hosts->count;
    return current;
}

void fatal(const char *str)
{
    if (errno)
        perror(str);
    else
        fprintf(stderr, "Error: %s\n", str);

    fflush(stderr);
    exit(1);
}

