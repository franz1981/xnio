/*
 * JBoss, Home of Professional Open Source
 *
 * Copyright 2013 Red Hat, Inc. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.xnio.nio;

import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static org.xnio.Bits.allAreClear;
import static org.xnio.Bits.allAreSet;
import static org.xnio.Bits.packInts;
import static org.xnio.Bits.unpackFirstInt;
import static org.xnio.Bits.unpackSecondInt;

/**
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
abstract class NioHandle {
    private static final AtomicLongFieldUpdater<NioHandle> KEY_OPS_UPDATER = AtomicLongFieldUpdater
            .newUpdater(NioHandle.class, "keyOps");
    private final WorkerThread workerThread;
    private final SelectionKey selectionKey;
    private static final int SET_OPS = 1;
    private static final int CLEAR_OPS = 2;
    private static final int NO_OPS = 0;
    private static final long NIL_KEY_OPS = packInts(NO_OPS, 0);

    // TODO We could do better here if we're 100% sure we're the only in charge of modifying the sel key int ops!!!!
    private volatile long keyOps = NIL_KEY_OPS;

    protected NioHandle(final WorkerThread workerThread, final SelectionKey selectionKey) {
        this.workerThread = workerThread;
        this.selectionKey = selectionKey;
    }

    void resume(final int ops) {
        try {
            setKeyOps(ops);
        } catch (CancelledKeyException ignored) {}
    }

    private void setKeyOps(int ops) {
        final SelectionKey key = this.selectionKey;
        final WorkerThread workerThread = this.workerThread;
        for (; ; ) {
            if (allAreSet(key.interestOps(), ops)) {
                return;
            }
            final long keyOps = this.keyOps;
            if (keyOps != NIL_KEY_OPS) {
                if (unpackFirstInt(keyOps) == SET_OPS && unpackSecondInt(keyOps) == ops) {
                    // someone is already doing it, no need to try
                    return;
                }
                // something is happening, let's observe the changes, maybe we will be lucky!
                // TODO: in case a wakeup and/or clear interrupts on select are issued, we eat too much cpu on spin!
                continue;
            }
            // let's try!
            if (!KEY_OPS_UPDATER.compareAndSet(this, NIL_KEY_OPS, packInts(SET_OPS, ops))) {
                // :( concurrent attempt, let's observe what's going on
                continue;
            }
            try {
                workerThread.setOps(key, ops);
            } finally {
                this.keyOps = NIL_KEY_OPS;
            }
            return;
        }
    }

    void wakeup(final int ops) {
        workerThread.queueTask(new Runnable() {
            public void run() {
                handleReady(ops);
            }
        });
        try {
            setKeyOps(ops);
        } catch (CancelledKeyException ignored) {}
    }

    void suspend(final int ops) {
        try {
            final SelectionKey key = this.selectionKey;
            final WorkerThread workerThread = this.workerThread;
            for (; ; ) {
                if (allAreClear(key.interestOps(), ops)) {
                    return;
                }
                final long keyOps = this.keyOps;
                if (keyOps != NIL_KEY_OPS) {
                    if (unpackFirstInt(keyOps) == CLEAR_OPS && unpackSecondInt(keyOps) == ops) {
                        // someone is already doing it, no need to try
                        return;
                    }
                    // something is happening, let's observe the changes, maybe we will be lucky!
                    // TODO: in case a wakeup is issued, we risk to spin for some time :"( need to fix this in prod!!!!!!!
                    continue;
                }
                // let's try!
                if (!KEY_OPS_UPDATER.compareAndSet(this, NIL_KEY_OPS, packInts(CLEAR_OPS, ops))) {
                    // :( concurrent attempt, let's observe what's going on
                    continue;
                }
                try {
                    workerThread.clearOps(key, ops);
                } finally {
                    this.keyOps = NIL_KEY_OPS;
                }
                return;
            }

        } catch (CancelledKeyException ignored) {}
    }

    boolean isResumed(final int ops) {
        try {
            return allAreSet(selectionKey.interestOps(), ops);
        } catch (CancelledKeyException ignored) {
            return false;
        }
    }

    abstract void handleReady(final int ops);

    abstract void forceTermination();

    abstract void terminated();

    WorkerThread getWorkerThread() {
        return workerThread;
    }

    SelectionKey getSelectionKey() {
        return selectionKey;
    }

    void cancelKey(final boolean block) {
        workerThread.cancelKey(selectionKey, block);
    }
}
