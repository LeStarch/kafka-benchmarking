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

import org.dia.benchmark.kafka.Aggregator;
import org.dia.benchmark.kafka.Configuration;

/**
 * This aggregator wraps another aggregator for network usage
 * providing an RMI interface to other aggregators.
 *
 * @author starchmd
 */
public class RmiAggregatorServer implements RmiAggregator {

	public static final String BIND_NAME = "RmiServer";

    Aggregator child;
    @Override
    public void spawn(Class<?> clazz) throws Exception {
		Constructor<?> ctor = clazz.getConstructor();
		child = (Aggregator)ctor.newInstance(new Object[] {});
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
    
    /**
     * Test main program.
     * @param args - command line arguments
     */
    public static void main(String[] args) {
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
        try {
            RmiAggregator agg = new RmiAggregatorServer();
            RmiAggregator stub = (RmiAggregator) UnicastRemoteObject.exportObject(agg,0);
            Registry registry = LocateRegistry.getRegistry(new Configuration().RMI_PORT);
            registry.rebind(BIND_NAME, stub);
        } catch (Exception e) {
            System.err.println("Exception:"+e);
            e.printStackTrace();
        }
    }
}
