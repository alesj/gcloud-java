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

import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.concurrent.Callable;

import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.TransactionManager;

class JTAAdapter {
    private static TransactionManagerLookup tml;

    private static synchronized TransactionManagerLookup getTransactionManagerLookup() {
        if (tml == null) {
            ServiceLoader<TransactionManagerLookup> loader = ServiceLoader.load(TransactionManagerLookup.class);
            Iterator<TransactionManagerLookup> iterator = loader.iterator();
            if (iterator.hasNext()) {
                tml = iterator.next();
            } else {
                tml = new GenericTransactionManagerLookup();
            }
        }
        return tml;
    }

    static Callable<Transaction.Response> attach(final Transaction transaction) {
        try {
            TransactionManager tm = getTransactionManagerLookup().getTransactionManager();
            javax.transaction.Transaction tx = tm.getTransaction();
            if (tx != null) {
                TxSync sync = new TxSync(transaction);
                tx.registerSynchronization(sync);
                return sync;
            } else {
                return new Callable<Transaction.Response>() {
                    public Transaction.Response call() throws Exception {
                        return transaction.commit();
                    }
                };
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot attach to JTA transaction.", e);
        }
    }

    private static class TxSync implements Synchronization, Callable<Transaction.Response> {
        private final Transaction transaction;
        private Transaction.Response response;

        public TxSync(Transaction transaction) {
            this.transaction = transaction;
        }

        public void beforeCompletion() {
            // try to commit
            response = transaction.commit();
        }

        public void afterCompletion(int status) {
            if (status == Status.STATUS_ROLLEDBACK) {
                transaction.rollback();
            }
        }

        public Transaction.Response call() throws Exception {
            return response;
        }
    }
}
