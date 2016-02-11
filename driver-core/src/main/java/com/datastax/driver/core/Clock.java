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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A small abstraction around system clock that aims to provide microsecond precision with the best accuracy possible.
 */
interface Clock {

    /**
     * Returns the current time in microseconds.
     *
     * @return the difference, measured in microseconds, between the current time and and the Epoch
     * (that is, midnight, January 1, 1970 UTC).
     */
    long currentTimeMicros();
}

/**
 * Factory that returns the best Clock implementation depending on what native libraries are available in the system.
 * If LibC is available through JNR, and if the system property {@code com.datastax.driver.USE_NATIVE_CLOCK} is set to {@code true}
 * (which is the default value), then {@link NativeClock} is returned, otherwise {@link SystemClock} is returned.
 */
class ClockFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClockFactory.class);

    private static final String USE_NATIVE_CLOCK_SYSTEM_PROPERTY = "com.datastax.driver.USE_NATIVE_CLOCK";

    static Clock newInstance() {
        if (Native.isLibCLoaded() && SystemProperties.getBoolean(USE_NATIVE_CLOCK_SYSTEM_PROPERTY, true)) {
            LOGGER.debug("Using Native clock to generate timestamps (microsecond precision)");
            return new NativeClock();
        } else {
            LOGGER.debug("Using System clock to generate timestamps (millisecond precision)");
            return new SystemClock();
        }
    }

}

/**
 * Default implementation of a clock that delegate its calls to the system clock.
 *
 * @see System#currentTimeMillis()
 */
class SystemClock implements Clock {

    @Override
    public long currentTimeMicros() {
        return System.currentTimeMillis() * 1000;
    }

}

/**
 * Provides the current time with microseconds precision with some reasonable accuracy through
 * the use of {@link Native#currentTimeMicros()}.
 * <p/>
 * But because calling JNR methods is slightly expensive (roughly 300 ns for {@link Native#currentTimeMicros()}),
 * we only call it once per {@code UPDATE_INTERVAL_MS}
 * and add the number of nanoseconds since the last call to get the current time, which is good
 * enough accuracy for our purpose (see CASSANDRA-6106).
 * <p/>
 * This reduces the cost of the call to {@link NativeClock#currentTimeMicros()} to roughly 30 ns, which
 * is comparable to a call to {@link System#currentTimeMillis()}.
 */
class NativeClock implements Clock {

    private static final Logger LOGGER = LoggerFactory.getLogger(NativeClock.class);
    private static final long UPDATE_INTERVAL_MS = 1000;
    private static final TimeFetcher TIME_FETCHER = new TimeFetcher();

    static {
        Thread updater = new Thread(TIME_FETCHER, "Native Clock Fetcher");
        updater.setDaemon(true);
        updater.start();
    }

    /**
     * @return an approximation of the real-time in microseconds.
     */
    public long currentTimeMicros() {
        return TIME_FETCHER.currentTimeMicros();
    }

    private static class TimeFetcher implements Runnable {

        /**
         * Records a time in micros along with the nanoTime() value at the time the
         * time is fetch.
         */
        private static class FetchedTime {

            private final long timeInMicros;
            private final long nanoTimeAtCheck;

            private FetchedTime(long timeInMicros, long nanoTimeAtCheck) {
                this.timeInMicros = timeInMicros;
                this.nanoTimeAtCheck = nanoTimeAtCheck;
            }
        }

        private AtomicReference<FetchedTime> lastFetchedTime = new AtomicReference<FetchedTime>(fetchTimeMicros());

        @SuppressWarnings("InfiniteLoopStatement")
        @Override
        public void run() {
            while (true) {
                try {
                    lastFetchedTime.set(fetchTimeMicros());
                    Thread.sleep(UPDATE_INTERVAL_MS);
                } catch (Exception e) {
                    LOGGER.error("Unexpected error while fetching current time", e);
                }
            }
        }

        long currentTimeMicros() {
            FetchedTime spec = lastFetchedTime.get();
            long curNano = System.nanoTime();
            return spec.timeInMicros + ((curNano - spec.nanoTimeAtCheck) / 1000);
        }

        private FetchedTime fetchTimeMicros() {
            // To compensate for the fact that the Native.currentTimeMicros call could take
            // some time, instead of picking the nano time before the call or after the
            // call, we take the average of both.
            long start = System.nanoTime();
            long micros = Native.currentTimeMicros();
            long end = System.nanoTime();
            // If it turns out the call took us more than 1 milliseconds (can happen while
            // the JVM warms up, unlikely otherwise, but no reasons to take risks), fall back
            // to System.currentMillis() temporarily
            if ((end - start) > 1000000)
                return new FetchedTime(System.currentTimeMillis() * 1000, System.nanoTime());
            return new FetchedTime(micros, (end + start) / 2);
        }

    }
}
