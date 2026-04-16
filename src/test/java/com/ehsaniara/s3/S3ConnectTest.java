/*
 * Copyright 2020 Jay Ehsaniara
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

package com.ehsaniara.s3;

import org.apache.maven.wagon.authentication.AuthenticationInfo;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import static org.junit.jupiter.api.Assertions.*;

class S3ConnectTest {

    @Test
    void connect_withDefaultCredentials_reusesClientInstance() throws Exception {
        S3Client first = S3Connect.connect(null, "us-east-1", EndpointProperty.empty(), new PathStyleEnabledProperty("false"), null);
        S3Client second = S3Connect.connect(null, "us-east-1", EndpointProperty.empty(), new PathStyleEnabledProperty("false"), null);

        assertSame(first, second, "Expected cached S3Client for default credentials flow");
    }

    @Test
    void connect_withProfile_reusesClientInstance() throws Exception {
        S3Client first = S3Connect.connect(null, "us-east-1", EndpointProperty.empty(), new PathStyleEnabledProperty("false"), "maven");
        S3Client second = S3Connect.connect(null, "us-east-1", EndpointProperty.empty(), new PathStyleEnabledProperty("false"), "maven");

        assertSame(first, second, "Expected cached S3Client for profile credentials flow");
    }

    @Test
    void connect_withDifferentProfiles_returnsDifferentClients() throws Exception {
        S3Client first = S3Connect.connect(null, "us-east-1", EndpointProperty.empty(), new PathStyleEnabledProperty("false"), "maven");
        S3Client second = S3Connect.connect(null, "us-east-1", EndpointProperty.empty(), new PathStyleEnabledProperty("false"), "another-profile");

        assertNotSame(first, second, "Different profiles should use different cached clients");
    }

    @Test
    void connect_withAuthenticationInfo_reusesClientInstance() throws Exception {
        AuthenticationInfo authInfo = new AuthenticationInfo();
        authInfo.setUserName("access");
        authInfo.setPassword("secret");

        S3Client first = S3Connect.connect(authInfo, "us-east-1", EndpointProperty.empty(), new PathStyleEnabledProperty("false"), null);
        S3Client second = S3Connect.connect(authInfo, "us-east-1", EndpointProperty.empty(), new PathStyleEnabledProperty("false"), null);

        assertSame(first, second, "Expected cached S3Client for auth-info flow");
    }

    @Test
    void connect_withDifferentAuthenticationInfo_returnsDifferentClients() throws Exception {
        AuthenticationInfo firstAuth = new AuthenticationInfo();
        firstAuth.setUserName("access-a");
        firstAuth.setPassword("secret-a");

        AuthenticationInfo secondAuth = new AuthenticationInfo();
        secondAuth.setUserName("access-b");
        secondAuth.setPassword("secret-b");

        S3Client first = S3Connect.connect(firstAuth, "us-east-1", EndpointProperty.empty(), new PathStyleEnabledProperty("false"), null);
        S3Client second = S3Connect.connect(secondAuth, "us-east-1", EndpointProperty.empty(), new PathStyleEnabledProperty("false"), null);

        assertNotSame(first, second, "Different auth-info values should not share a cached client");
    }

    @Test
    void connect_cachedClientClose_doesNotEvictOrBreakReuse() throws Exception {
        S3Client first = S3Connect.connect(null, "us-east-1", EndpointProperty.empty(), new PathStyleEnabledProperty("false"), null);

        assertDoesNotThrow(first::close);

        S3Client second = S3Connect.connect(null, "us-east-1", EndpointProperty.empty(), new PathStyleEnabledProperty("false"), null);
        assertSame(first, second, "close() on cached client should not close the shared instance");
    }
}
