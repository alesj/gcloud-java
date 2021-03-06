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

package com.google.gcloud;


import static com.google.common.base.MoreObjects.firstNonNull;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.extensions.appengine.http.UrlFetchTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLConnection;
import java.util.Set;

public abstract class ServiceOptions {

  private static final String DEFAULT_HOST = "https://www.googleapis.com";

  private final String host;
  private final HttpTransport httpTransport;
  private final AuthConfig authConfig;
  private final RetryParams retryParams;

  protected abstract static class Builder<B extends Builder<B>> {

    private String host;
    private HttpTransport httpTransport;
    private AuthConfig authConfig;
    private RetryParams retryParams;

    protected Builder() {}

    protected Builder(ServiceOptions options) {
      host = options.host;
      httpTransport = options.httpTransport;
      authConfig = options.authConfig;
      retryParams = options.retryParams;
    }

    protected abstract ServiceOptions build();

    @SuppressWarnings("unchecked")
    protected B self() {
      return (B) this;
    }

    public B host(String host) {
      this.host = host;
      return self();
    }

    public B httpTransport(HttpTransport httpTransport) {
      this.httpTransport = httpTransport;
      return self();
    }

    public B authConfig(AuthConfig authConfig) {
      this.authConfig = authConfig;
      return self();
    }

    public B retryParams(RetryParams retryParams) {
      this.retryParams = retryParams;
      return self();
    }
  }

  protected ServiceOptions(Builder<?> builder) {
    host = firstNonNull(builder.host, DEFAULT_HOST);
    httpTransport = firstNonNull(builder.httpTransport, defaultHttpTransport());
    authConfig = firstNonNull(builder.authConfig, defaultAuthConfig());
    retryParams = builder.retryParams;
  }

  private static HttpTransport defaultHttpTransport() {
    // Consider App Engine
    if (appEngineAppId() != null) {
      try {
        return new UrlFetchTransport();
      } catch (Exception ignore) {
        // Maybe not on App Engine
      }
    }
    // Consider Compute
    try {
      return AuthConfig.getComputeCredential().getTransport();
    } catch (Exception e) {
      // Maybe not on GCE
    }
    return new NetHttpTransport();
  }

  private static AuthConfig defaultAuthConfig() {
    // Consider App Engine
    if (appEngineAppId() != null) {
      try {
        return AuthConfig.createForAppEngine();
      } catch (Exception ignore) {
        // Maybe not on App Engine
      }
    }
    // Consider Compute
    try {
      return AuthConfig.createForComputeEngine();
    } catch (Exception ignore) {
      // Maybe not on GCE
    }
    return AuthConfig.noCredentials();
  }

  protected static String appEngineAppId() {
    return System.getProperty("com.google.appengine.application.id");
  }

  protected static String googleCloudProjectId() {
    try {
      URL url = new URL("http://metadata/computeMetadata/v1/project/project-id");
      URLConnection connection = url.openConnection();
      connection.setRequestProperty("X-Google-Metadata-Request", "True");
      try (BufferedReader reader =
               new BufferedReader(new InputStreamReader(connection.getInputStream(), UTF_8))) {
        return reader.readLine();
      }
    } catch (IOException ignore) {
      // return null if can't determine
      return null;
    }
  }

  protected static String getAppEngineProjectId() {
    // TODO(ozarov): An alternative to reflection would be to depend on AE api jar:
    // http://mvnrepository.com/artifact/com.google.appengine/appengine-api-1.0-sdk/1.2.0
    try {
      Class<?> factoryClass =
          Class.forName("com.google.appengine.api.appidentity.AppIdentityServiceFactory");
      Method method = factoryClass.getMethod("getAppIdentityService");
      Object appIdentityService = method.invoke(null);
      method = appIdentityService.getClass().getMethod("getServiceAccountName");
      String serviceAccountName = (String) method.invoke(appIdentityService);
      int indexOfAtSign = serviceAccountName.indexOf('@');
      return serviceAccountName.substring(0, indexOfAtSign);
    } catch (Exception ignore) {
      // return null if can't determine
      return null;
    }
  }

  protected abstract Set<String> scopes();

  public String host() {
    return host;
  }

  public HttpTransport httpTransport() {
    return httpTransport;
  }

  public AuthConfig authConfig() {
    return authConfig;
  }

  public RetryParams retryParams() {
    return retryParams;
  }

  protected HttpRequestInitializer httpRequestInitializer() {
    return authConfig().httpRequestInitializer(httpTransport, scopes());
  }

  public abstract Builder<?> toBuilder();
}
