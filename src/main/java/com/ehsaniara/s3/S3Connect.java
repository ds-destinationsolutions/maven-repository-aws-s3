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

import lombok.extern.java.Log;
import org.apache.maven.wagon.authentication.AuthenticationException;
import org.apache.maven.wagon.authentication.AuthenticationInfo;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * S3Connect s3Connect
 *
 * @author jay
 * @version $Id: $Id
 */
@Log
public class S3Connect {

    private static final Object CLIENT_CACHE_LOCK = new Object();
    private static final Map<CacheKey, WeakReference<S3Client>> CLIENT_CACHE = new HashMap<>();

    /**
     * <p>connect.</p>
     *
     * @param authenticationInfo authenticationInfo
     * @param region             region
     * @param endpoint           endpoint
     * @param pathStyle          pathStyle
     * @return S3Client
     * @throws org.apache.maven.wagon.authentication.AuthenticationException org.apache.maven.wagon.authentication.AuthenticationException
     */
    public static S3Client connect(AuthenticationInfo authenticationInfo, String region, EndpointProperty endpoint, PathStyleEnabledProperty pathStyle, String profile) throws AuthenticationException {

        try {
            S3Client s3Client = getOrCreateCachedClient(authenticationInfo, region, endpoint, pathStyle, profile);

            log.finer(String.format("Connected to S3 using endpoint %s.", endpoint.isPresent() ? endpoint.get() : "default"));

            return s3Client;
        } catch (SdkClientException e) {
            StringBuilder errorMessage = new StringBuilder();
            errorMessage.append("Failed to connect");
            if (endpoint.isPresent()) {
                errorMessage.append(String.format(" to endpoint [%s]", endpoint.get()));
            }
            if (region != null) {
                errorMessage.append(String.format(" using region [%s]", region));
            }
            throw new AuthenticationException(errorMessage.toString(), e);
        }
    }

    /**
     * @param authenticationInfo authenticationInfo
     * @param region             region
     * @param endpoint           endpoint
     * @param pathStyle          pathStyle
     * @return S3Client
     */
    private static S3Client createS3Client(AuthenticationInfo authenticationInfo, String region, EndpointProperty endpoint, PathStyleEnabledProperty pathStyle, String profile) {
        final S3RegionProviderOrder regionProvider = new S3RegionProviderOrder(region);

        log.fine("Creating new S3Client instance.");

        S3Configuration s3Config = S3Configuration.builder()
                .pathStyleAccessEnabled(pathStyle.get())
                .build();

        S3ClientBuilder builder = S3Client.builder()
                .credentialsProvider(new AwsCredentialsFactory().connect(authenticationInfo, profile))
                .serviceConfiguration(s3Config);

        String regionId = regionProvider.getRegionString();
        if (regionId != null) {
            builder.region(Region.of(regionId));
        } else {
            // Default fallback region
            builder.region(Region.US_EAST_1);
        }

        if (endpoint.isPresent()) {
            builder.endpointOverride(URI.create(endpoint.get()));
        }

        return builder.build();
    }

    private static S3Client getOrCreateCachedClient(AuthenticationInfo authenticationInfo, String region, EndpointProperty endpoint, PathStyleEnabledProperty pathStyle, String profile) {
        final CacheKey cacheKey = new CacheKey(
                region,
                endpoint.isPresent() ? endpoint.get() : null,
                pathStyle.get(),
                profile,
                authenticationInfo == null ? null : authenticationInfo.getUserName(),
                authenticationInfo == null ? null : authenticationInfo.getPassword());
        synchronized (CLIENT_CACHE_LOCK) {
            WeakReference<S3Client> cachedRef = CLIENT_CACHE.get(cacheKey);
            S3Client cachedClient = cachedRef == null ? null : cachedRef.get();
            if (cachedClient != null) {
                return cachedClient;
            }

            S3Client client = createS3Client(authenticationInfo, region, endpoint, pathStyle, profile);
            S3Client nonClosingClient = createNonClosingClient(client);
            CLIENT_CACHE.put(cacheKey, new WeakReference<>(nonClosingClient));
            CLIENT_CACHE.entrySet().removeIf(entry -> entry.getValue().get() == null);
            return nonClosingClient;
        }
    }

    private static S3Client createNonClosingClient(S3Client delegate) {
        return (S3Client) Proxy.newProxyInstance(
                S3Client.class.getClassLoader(),
                new Class[]{S3Client.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
                        log.finer("Ignoring close() on cached shared S3Client instance.");
                        return null;
                    }

                    try {
                        return method.invoke(delegate, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                }
        );
    }

    private static final class CacheKey {

        private final String region;
        private final String endpoint;
        private final boolean pathStyle;
        private final String profile;
        private final String authUserName;
        private final String authPassword;

        private CacheKey(String region, String endpoint, boolean pathStyle, String profile, String authUserName, String authPassword) {
            this.region = region;
            this.endpoint = endpoint;
            this.pathStyle = pathStyle;
            this.profile = profile;
            this.authUserName = authUserName;
            this.authPassword = authPassword;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CacheKey)) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return pathStyle == cacheKey.pathStyle
                    && Objects.equals(region, cacheKey.region)
                    && Objects.equals(endpoint, cacheKey.endpoint)
                    && Objects.equals(profile, cacheKey.profile)
                    && Objects.equals(authUserName, cacheKey.authUserName)
                    && Objects.equals(authPassword, cacheKey.authPassword);
        }

        @Override
        public int hashCode() {
            return Objects.hash(region, endpoint, pathStyle, profile, authUserName, authPassword);
        }
    }
}
