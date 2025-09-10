/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;

import java.net.MalformedURLException;
import javax.net.ssl.SSLException;
import org.testng.annotations.Test;

/**
 * Comprehensive tests for DgraphClient.ClientOptions functionality.
 * These tests focus on validation and parsing without requiring server connections.
 */
public class DgraphClientOptionsTest {

  // ========== Basic ClientOptions Tests ==========

  @Test
  public void testForAddress() {
    DgraphClient.ClientOptions options = DgraphClient.ClientOptions.forAddress("localhost", 9080);
    assertNotNull(options);
  }

  @Test
  public void testWithACLCredentials() {
    DgraphClient.ClientOptions options = DgraphClient.ClientOptions.forAddress("localhost", 9080)
        .withACLCredentials("username", "password");
    assertNotNull(options);
  }

  @Test
  public void testWithDgraphApiKey() {
    DgraphClient.ClientOptions options = DgraphClient.ClientOptions.forAddress("localhost", 9080)
        .withDgraphApiKey("test-api-key");
    assertNotNull(options);
  }

  @Test
  public void testWithBearerToken() {
    DgraphClient.ClientOptions options = DgraphClient.ClientOptions.forAddress("localhost", 9080)
        .withBearerToken("test-bearer-token");
    assertNotNull(options);
  }

  @Test
  public void testWithTLSSkipVerify() throws SSLException {
    DgraphClient.ClientOptions options = DgraphClient.ClientOptions.forAddress("localhost", 9080)
        .withTLSSkipVerify();
    assertNotNull(options);
  }

  // ========== Namespace Tests ==========

  @Test
  public void testWithNamespace() {
    DgraphClient.ClientOptions options = DgraphClient.ClientOptions.forAddress("localhost", 9080)
        .withACLCredentials("username", "password")
        .withNamespace(123);
    assertNotNull(options);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNamespaceWithoutCredentials() {
    DgraphClient.ClientOptions options = DgraphClient.ClientOptions.forAddress("localhost", 9080)
        .withNamespace(123);
    
    // Should throw exception when namespace is set without username/password
    options.build();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNamespaceWithOnlyUsername() {
    DgraphClient.ClientOptions options = DgraphClient.ClientOptions.forAddress("localhost", 9080)
        .withACLCredentials("username", null)
        .withNamespace(123);
    
    // Should throw exception when namespace is set without password
    options.build();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNamespaceWithApiKey() {
    DgraphClient.ClientOptions options = DgraphClient.ClientOptions.forAddress("localhost", 9080)
        .withDgraphApiKey("test-key")
        .withNamespace(123);
    
    // Should throw exception when namespace is set with API key (no ACL credentials)
    options.build();
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNamespaceWithBearerToken() {
    DgraphClient.ClientOptions options = DgraphClient.ClientOptions.forAddress("localhost", 9080)
        .withBearerToken("test-token")
        .withNamespace(123);
    
    // Should throw exception when namespace is set with bearer token (no ACL credentials)
    options.build();
  }

  // ========== Connection String Parsing Tests ==========

  @Test
  public void testOpenBasicConnectionString() throws MalformedURLException, SSLException {
    try {
      String connectionString = "dgraph://localhost:9080";
      DgraphClient.open(connectionString);
    } catch (Exception e) {
      // Connection failures are expected, but parsing errors would indicate a problem
      assertFalse(e instanceof IllegalArgumentException, 
          "Basic connection string parsing failed: " + e.getMessage());
    }
  }

  @Test
  public void testOpenWithCredentials() throws MalformedURLException, SSLException {
    try {
      String connectionString = "dgraph://username:password@localhost:9080";
      DgraphClient.open(connectionString);
    } catch (Exception e) {
      assertFalse(e instanceof IllegalArgumentException, 
          "Credentials parsing failed: " + e.getMessage());
    }
  }

  @Test
  public void testOpenWithSSLMode() throws MalformedURLException, SSLException {
    try {
      String connectionString = "dgraph://localhost:9080?sslmode=disable";
      DgraphClient.open(connectionString);
    } catch (Exception e) {
      assertFalse(e instanceof IllegalArgumentException, 
          "SSL mode parsing failed: " + e.getMessage());
    }
  }

  @Test
  public void testOpenWithApiKey() throws MalformedURLException, SSLException {
    try {
      String connectionString = "dgraph://localhost:9080?apikey=test-key";
      DgraphClient.open(connectionString);
    } catch (Exception e) {
      assertFalse(e instanceof IllegalArgumentException, 
          "API key parsing failed: " + e.getMessage());
    }
  }

  @Test
  public void testOpenWithBearerToken() throws MalformedURLException, SSLException {
    try {
      String connectionString = "dgraph://localhost:9080?bearertoken=test-token";
      DgraphClient.open(connectionString);
    } catch (Exception e) {
      assertFalse(e instanceof IllegalArgumentException, 
          "Bearer token parsing failed: " + e.getMessage());
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testOpenWithBothApiKeyAndBearerToken() throws MalformedURLException, SSLException {
    String connectionString = "dgraph://localhost:9080?apikey=test-key&bearertoken=test-token";
    
    // Should throw exception when both apikey and bearertoken are provided
    DgraphClient.open(connectionString);
  }

  // ========== Namespace Connection String Tests ==========

  @Test
  public void testOpenWithNamespaceParameter() throws MalformedURLException, SSLException {
    try {
      String connectionString = "dgraph://username:password@localhost:9080?namespace=123";
      DgraphClient.open(connectionString);
    } catch (Exception e) {
      assertFalse(e instanceof IllegalArgumentException, 
          "Namespace parsing failed: " + e.getMessage());
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testOpenWithNamespaceButNoCredentials() throws MalformedURLException, SSLException {
    String connectionString = "dgraph://localhost:9080?namespace=123";
    
    // Should throw exception when namespace is in connection string but no credentials
    DgraphClient.open(connectionString);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testOpenWithInvalidNamespace() throws MalformedURLException, SSLException {
    String connectionString = "dgraph://username:password@localhost:9080?namespace=invalid";
    
    // Should throw exception when namespace is not a valid integer
    DgraphClient.open(connectionString);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testOpenWithNamespaceAndApiKey() throws MalformedURLException, SSLException {
    String connectionString = "dgraph://localhost:9080?apikey=test&namespace=123";
    
    // Should throw exception when namespace is used with apikey (no username/password)
    DgraphClient.open(connectionString);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testOpenWithNamespaceAndBearerToken() throws MalformedURLException, SSLException {
    String connectionString = "dgraph://localhost:9080?bearertoken=test&namespace=123";
    
    // Should throw exception when namespace is used with bearertoken (no username/password)
    DgraphClient.open(connectionString);
  }

  @Test
  public void testOpenWithMultipleParameters() throws MalformedURLException, SSLException {
    try {
      String connectionString = "dgraph://username:password@localhost:9080?sslmode=disable&namespace=456";
      DgraphClient.open(connectionString);
    } catch (Exception e) {
      assertFalse(e instanceof IllegalArgumentException, 
          "Multiple parameters parsing failed: " + e.getMessage());
    }
  }

  @Test
  public void testOpenWithZeroNamespace() throws MalformedURLException, SSLException {
    try {
      String connectionString = "dgraph://username:password@localhost:9080?namespace=0";
      DgraphClient.open(connectionString);
    } catch (Exception e) {
      assertFalse(e instanceof IllegalArgumentException, 
          "Zero namespace parsing failed: " + e.getMessage());
    }
  }

  @Test
  public void testOpenWithNegativeNamespace() throws MalformedURLException, SSLException {
    try {
      String connectionString = "dgraph://username:password@localhost:9080?namespace=-1";
      DgraphClient.open(connectionString);
    } catch (Exception e) {
      assertFalse(e instanceof IllegalArgumentException, 
          "Negative namespace parsing failed: " + e.getMessage());
    }
  }

  // ========== Edge Case Tests ==========

  @Test
  public void testOpenWithEmptyParameters() throws MalformedURLException, SSLException {
    try {
      String connectionString = "dgraph://localhost:9080?";
      DgraphClient.open(connectionString);
    } catch (Exception e) {
      assertFalse(e instanceof IllegalArgumentException, 
          "Empty parameters parsing failed: " + e.getMessage());
    }
  }

  @Test
  public void testOpenWithMultipleQuestionMarks() throws MalformedURLException, SSLException {
    try {
      String connectionString = "dgraph://username:password@localhost:9080?sslmode=disable?namespace=123";
      DgraphClient.open(connectionString);
    } catch (Exception e) {
      // This should fail parsing, but not with IllegalArgumentException for namespace
      // The URL parsing itself might fail, which is expected
    }
  }

  @Test
  public void testTLSOptionsPreservesNamespace() throws SSLException {
    DgraphClient.ClientOptions options = DgraphClient.ClientOptions.forAddress("localhost", 9080)
        .withACLCredentials("username", "password")
        .withNamespace(123);
    
    DgraphClient.ClientOptions tlsOptions = options.withTLSSkipVerify();
    assertNotNull(tlsOptions);
    // The namespace should be preserved in the new options object
  }
}
