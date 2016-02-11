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

import com.google.common.annotations.VisibleForTesting;

/**
 * Base implementation for monotonic timestamp generators.
 */
abstract class AbstractMonotonicTimestampGenerator implements TimestampGenerator {

    @VisibleForTesting
    volatile Clock clock = ClockFactory.newInstance();

    /**
     * Compute the next timestamp, given the last timestamp previously generated.
     * The returned timestamp should be strictly greater than the previous one.
     * <p/>
     * This implementation is inspired by {@code org.apache.cassandra.service.ClientState#getTimestamp()}
     *
     * @return The next timestamp to use
     */
    protected long computeNext(long last) {
        long current = clock.currentTimeMicros();
        return last >= current ? last + 1 : current;
    }

}
