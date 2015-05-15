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

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * This aggregator wraps another aggregator for network usage.
 *
 * @author starchmd
 */
public class NetworkAggregator implements Aggregator {

    Aggregator child;

    /**
     * ctor
     * @param config - configuration object used to find RMI registry port
     * @param clazz - aggregator class to handle
     * @param host - hostname to connect to
     * @throws Exception - exception is thrown connecting to RMI
     */
    public NetworkAggregator(Configuration config,Class<?> clazz, String host) throws Exception {
    	 if (System.getSecurityManager() == null) {
             System.setSecurityManager(new SecurityManager());
         }
         Registry registry = LocateRegistry.getRegistry(host, config.RMI_REGISTRY_PORT);
         RmiAggregator rmiAgg = (RmiAggregator) registry.lookup(RmiAggregatorServer.BIND_NAME);
         rmiAgg.spawn(clazz);
         child = rmiAgg;
    }

    @Override
    public void setup(Configuration config) throws Exception {
        child.setup(config);
    }
    @Override
    public void start() throws Exception {
        child.start();
    }
    @Override
    public long stop() throws Exception {
        return child.stop();
    }
    @Override
    public long count() throws Exception {
        return child.count();
    }
}
