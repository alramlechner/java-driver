/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A timestamp generator that guarantees monotonically increasing timestamps among all client threads.
 * <p/>
 * The accuracy of the generated timestamps is largely dependent on the
 * granularity of the underlying operating system's clock.
 * <p/>
 * Generally speaking, this granularity is the millisecond, and
 * the sub-millisecond part is simply a counter that gets incremented
 * until the next clock tick, as provided by {@link System#currentTimeMillis()}.
 * <p/>
 * On some systems, however, it is possible to have a better granularity by using a JNR
 * call to {@code gettimeofday}. The driver will use this system call automatically whenever
 * available, unless the system property {@code com.datastax.driver.USE_NATIVE_CLOCK} is
 * explicitly set to {@code false}.
 * <p/>
 * Beware that to guarantee monotonicity, if more than one call to {@link #next()}
 * is made within the same microsecond, or in the event of a system clock skew, this generator might
 * return timestamps that drift out in the future.
 */
public class AtomicMonotonicTimestampGenerator extends AbstractMonotonicTimestampGenerator {

    private AtomicLong lastRef = new AtomicLong(0);

    @Override
    public long next() {
        while (true) {
            long last = lastRef.get();
            long next = computeNext(last);
            if (lastRef.compareAndSet(last, next))
                return next;
        }
    }
}
