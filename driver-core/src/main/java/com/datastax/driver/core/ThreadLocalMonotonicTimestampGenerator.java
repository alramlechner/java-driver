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


/**
 * A timestamp generator that guarantees monotonically increasing timestamps
 * on a per-thread basis.
 * <p/>
 * Beware that there is a risk of timestamp collision with this generator when accessed
 * by more than one thread at a time; only use it when threads are not in direct competition
 * for timestamp ties (i.e., they are executing independent statements).
 * <p/>
 * The accuracy of the generated timestamps is largely dependent on the
 * granularity of the underlying operating system's clock.
 * <p/>
 * Generally speaking, this granularity is the millisecond, and
 * the sub-millisecond part is simply a counter that gets incremented
 * until the next clock tick, as provided by {@link System#currentTimeMillis()}.
 * <p/>
 * On Linux systems, however, it is possible to have a better granularity by using a JNA
 * call to {@code clock_gettime}. To benefit from this system call, set the system
 * property {@code com.datastax.driver.USE_NATIVE_CLOCK} to {@code true}.
 * <p/>
 * Beware that to guarantee monotonicity, if more than one call to {@link #next()}
 * is made within the same microsecond, or in the event of a system clock skew, this generator might
 * return timestamps that drift out in the future.
 */
public class ThreadLocalMonotonicTimestampGenerator extends AbstractMonotonicTimestampGenerator {

    // We're deliberately avoiding an anonymous subclass with initialValue(), because this can introduce
    // classloader leaks in managed environments like Tomcat
    private final ThreadLocal<Long> lastRef = new ThreadLocal<Long>();

    @Override
    public long next() {
        Long last = this.lastRef.get();
        if (last == null)
            last = 0L;

        long next = computeNext(last);

        this.lastRef.set(next);
        return next;
    }
}
