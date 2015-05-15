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
import java.lang.reflect.Constructor;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This aggregator wraps another aggregator for network usage
 * providing an RMI interface to other aggregators.
 *
 * @author starchmd
 */
public class RmiAggregatorServer implements RmiAggregator {

    private static final Logger log = Logger.getLogger(RmiAggregatorServer.class.getName());
    public static final String BIND_NAME = "RmiServer";

    Aggregator child;
    @Override
    public void spawn(Class<?> clazz) throws Exception {
        Constructor<?> ctor = clazz.getConstructor();
        child = (Aggregator)ctor.newInstance(new Object[] {});
    }

    @Override
    public void setup(Configuration config) throws Exception {
        log.log(Level.INFO, "Setting up RMI aggregator");
        child.setup(config);
        if (config.USE_MONITOR.equalsIgnoreCase("true")) {
            new Thread(new BandwidthAggregator.Monitor(child)).start();
        }
    }
    @Override
    public void start() throws Exception {
        log.log(Level.INFO, "Starting RMI aggregator");
        child.start();
    }
    @Override
    public long stop() throws Exception {
        log.log(Level.INFO, "Stoping RMI aggregator");
        return child.stop();
    }
    @Override
    public long count() throws Exception {
        log.log(Level.INFO, "Counting RMI aggregator");
        return child.count();
    }

    /**
     * Network main program.
     * @param args - command line arguments
     */
    public static void main(String[] args) {
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
        try {
            Configuration config = new Configuration(Configuration.getProperties());
            RmiAggregator agg = new RmiAggregatorServer();
            RmiAggregator stub = (RmiAggregator) UnicastRemoteObject.exportObject(agg,0);
            Registry registry = LocateRegistry.getRegistry(config.RMI_REGISTRY_PORT);
            registry.rebind(BIND_NAME, stub);
        } catch (IllegalAccessException e) {
            System.err.println("Illegal access exception: "+e);
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("I/O exception: "+e);
            e.printStackTrace();
        }
    }
}
