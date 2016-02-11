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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.utils.CassandraVersion;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.datastax.driver.core.ConsistencyLevel.SERIAL;
import static com.datastax.driver.core.ConsistencyLevel.TWO;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;

/**
 * Note: we can't extend {@link AbstractRetryPolicyIntegrationTest} here, because we are
 * testing LWT statements.
 */
@CCMConfig(
        numberOfNodes = 3,
        dirtiesContext = true,
        createCluster = false
)
@CassandraVersion(major = 2.0)
public class CASRetryPolicyIntegrationTest extends CCMTestsSupport {

    @Test(groups = "long")
    public void should_not_call_policy_with_serial_CL() {

        RetryPolicy retryPolicy = Mockito.spy(DefaultRetryPolicy.INSTANCE);
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withSocketOptions(new SocketOptions().setReadTimeoutMillis(120000))
                .withRetryPolicy(retryPolicy)
                .build());

        Session session = cluster.connect();

        ccm().stop(1);
        ccm().waitForDown(1);
        TestUtils.waitForDown(ccm().addressOfNode(1).getHostName(), cluster);

        ccm().stop(2);
        ccm().waitForDown(2);
        TestUtils.waitForDown(ccm().addressOfNode(2).getHostName(), cluster);

        String ks = TestUtils.generateIdentifier("ks_");
        session.execute(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}", ks));
        useKeyspace(session, ks);
        session.execute("CREATE TABLE foo(k int primary key, c int)");
        session.execute("INSERT INTO foo(k, c) VALUES (0, 0)");

        Statement s = new SimpleStatement("UPDATE foo SET c = 1 WHERE k = 0 IF c = 0")
                .setConsistencyLevel(TWO)
                .setSerialConsistencyLevel(SERIAL);

        try {
            session.execute(s);
            fail("Expected NoHostAvailableException");
        } catch (NoHostAvailableException e) {
            //ok
        }

        Mockito.verify(retryPolicy, times(1)).onUnavailable(
                eq(s), eq(TWO), eq(2), eq(1), eq(0));

    }

}
