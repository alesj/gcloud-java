/*
 * Copyright 2015 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.gcloud.datastore;

import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.TransactionManager;

/**
 * Taken from Infinispan project.
 */
class GenericTransactionManagerLookup implements TransactionManagerLookup {
    private static final Logger log = Logger.getLogger(GenericTransactionManagerLookup.class.getName());

    /**
     * JNDI lookups performed?
     */
    private boolean lookupDone = false;

    /**
     * No JNDI available?
     */
    private boolean lookupFailed = false;

    /**
     * The JTA TransactionManager found.
     */
    private TransactionManager tm = null;

    /**
     * JNDI locations for TransactionManagers we know of
     */
    private static String[][] knownJNDIManagers =
        {
            {"java:jboss/TransactionManager", "JBoss WildFly"},
            {"java:/TransactionManager", "JBoss AS 4 ~ 6, JRun4"},
            {"java:comp/TransactionManager", "Resin 3.x"},
            {"java:appserver/TransactionManager", "Sun Glassfish"},
            {"java:pm/TransactionManager", "Borland, Sun"},
            {"javax.transaction.TransactionManager", "BEA WebLogic"},
            {"java:comp/UserTransaction", "Resin, Orion, JOnAS (JOTM)"},
            {"osgi:service/javax.transaction.TransactionManager", "Karaf"},
        };

    /**
     * WebSphere 5.1 and 6.0 TransactionManagerFactory
     */
    private static final String WS_FACTORY_CLASS_5_1 = "com.ibm.ws.Transaction.TransactionManagerFactory";

    /**
     * WebSphere 5.0 TransactionManagerFactory
     */
    private static final String WS_FACTORY_CLASS_5_0 = "com.ibm.ejs.jts.jta.TransactionManagerFactory";

    /**
     * WebSphere 4.0 TransactionManagerFactory
     */
    private static final String WS_FACTORY_CLASS_4 = "com.ibm.ejs.jts.jta.JTSXA";

    /**
     * Get the systemwide used TransactionManager
     *
     * @return TransactionManager
     */
    public synchronized TransactionManager getTransactionManager() {
        if (!lookupDone) {
            doLookups();
        }
        if (tm != null) {
            return tm;
        }
        throw new IllegalStateException("No TransactionManager could be found, use custom TransactionManagerLookup.");
    }

    private static void quietClose(Context context) {
        try {
            if (context != null) {
                context.close();
            }
        } catch (Exception ignored) {
        }
    }

    /**
     * Try to figure out which TransactionManager to use
     */
    private void doLookups() {
        if (lookupFailed) {
            return;
        }

        InitialContext ctx = null;
        try {
            ctx = new InitialContext();
        } catch (NamingException e) {
            lookupFailed = true;
            quietClose(ctx);
            return;
        }

        try {
            //probe jndi lookups first
            for (String[] knownJNDIManager : knownJNDIManagers) {
                Object jndiObject;
                try {
                    log.fine(String.format("Trying to lookup TransactionManager for %s", knownJNDIManager[1]));
                    jndiObject = ctx.lookup(knownJNDIManager[0]);
                } catch (NamingException e) {
                    log.fine(String.format("Failed to perform a lookup for [%s (%s)]", knownJNDIManager[0], knownJNDIManager[1]));
                    continue;
                }
                if (jndiObject instanceof TransactionManager) {
                    tm = (TransactionManager) jndiObject;
                    log.fine(String.format("Found TransactionManager for %s", knownJNDIManager[1]));
                    return;
                }
            }
        } finally {
            quietClose(ctx);
        }

        //try to find websphere lookups since we came here

        // try this classloader
        doLookup(GenericTransactionManagerLookup.class.getClassLoader());
        // try the TCCL / app
        if (tm == null) {
            doLookup(Thread.currentThread().getContextClassLoader());
        }

        lookupDone = true;
    }

    private void doLookup(ClassLoader cl) {
        Class<?> clazz;
        try {
            log.fine(String.format("Trying WebSphere 5.1: %s", WS_FACTORY_CLASS_5_1));
            clazz = loadClassStrict(WS_FACTORY_CLASS_5_1, cl);
            log.fine(String.format("Found WebSphere 5.1: %s", WS_FACTORY_CLASS_5_1));
        } catch (ClassNotFoundException ex) {
            try {
                log.fine(String.format("Trying WebSphere 5.0: %s", WS_FACTORY_CLASS_5_0));
                clazz = loadClassStrict(WS_FACTORY_CLASS_5_0, cl);
                log.fine(String.format("Found WebSphere 5.0: %s", WS_FACTORY_CLASS_5_0));
            } catch (ClassNotFoundException ex2) {
                try {
                    log.fine(String.format("Trying WebSphere 4: %s", WS_FACTORY_CLASS_4));
                    clazz = loadClassStrict(WS_FACTORY_CLASS_4, cl);
                    log.fine(String.format("Found WebSphere 4: %s", WS_FACTORY_CLASS_4));
                } catch (ClassNotFoundException ex3) {
                    log.fine("Couldn't find any WebSphere TransactionManager factory class, neither for WebSphere version 5.1 nor 5.0 nor 4");
                    lookupFailed = true;
                    return;
                }
            }
        }
        try {
            Class<?>[] signature = null;
            Object[] args = null;
            Method method = clazz.getMethod("getTransactionManager", signature);
            tm = (TransactionManager) method.invoke(null, args);
        } catch (Exception ex) {
            log.log(Level.INFO, "Unable to invoke Websphere static getTm method: " + clazz.getName(), ex);
        }
    }

    private static Class<?> loadClassStrict(String className, ClassLoader cl) throws ClassNotFoundException {
        return cl.loadClass(className);
    }

}
