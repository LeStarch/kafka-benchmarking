/*
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements.  See the NOTICE.txt file distributed with this work for
additional information regarding copyright ownership.  The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License.  You may obtain a copy of
the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
License for the specific language governing permissions and limitations under
the License.
 */
package org.dia.benchmark.kafka;

import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.dia.benchmark.kafka.Aggregator;
import org.dia.benchmark.kafka.Configuration;

/**
 * This aggregator wraps another aggregator for network usage.
 *
 * @author starchmd
 */
public class NetworkAggregator implements Aggregator {

    Aggregator child;

    /**
     * ctor
     * @param clazz - aggregator class to handle
     * @param host - hostname to connect to
     */
    public NetworkAggregator(Configuration config,Class<?> clazz, String host) throws Exception {
    	 if (System.getSecurityManager() == null) {
             System.setSecurityManager(new SecurityManager());
         }
         Registry registry = LocateRegistry.getRegistry(host, config.RMI_PORT);
         child = (RmiAggregator) registry.lookup(RmiAggregatorServer.BIND_NAME);
    }

    @Override
    public void setup(Configuration config) throws IOException {
        child.setup(config);
    }
    @Override
    public void start() throws IOException {
        child.start();
    }
    @Override
    public long stop() throws IOException {
        return child.stop();
    }
    @Override
    public long count() throws IOException {
        return child.count();
    }
}
