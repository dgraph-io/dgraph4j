/*
 * SPDX-FileCopyrightText: © Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

import static org.testng.Assert.*;
import static org.testng.Assert.fail;

import io.dgraph.DgraphProto.Response;
import org.testng.annotations.Test;

/**
 * @author Matthew McNeely
 */
public class DgraphConnTest {

  // Define a constant for the expected JSON response
  private static final String EXPECTED_UID_JSON = "{\"me\":[{\"uid\":\"0x1\"}]}";

  /**
   * Tests the Dgraph connection using the open method with ACL credentials.
   * This test connects to a local Dgraph server with authentication.
   */
  @Test
  public void testOpenWithACL() {
    try {
      String conn = "dgraph://groot:password@localhost:9180";
      DgraphClient client = DgraphClient.open(conn);
      assertNotNull(client);
      
      String query = "{ me(func: uid(1)) { uid }}";
      Response response = client.newTransaction().query(query);
      assertNotNull(response);
      assertEquals(response.getJson().toStringUtf8(), EXPECTED_UID_JSON);
      
      client.shutdown();
    } catch (Exception e) {
      fail("Failed to connect with ACL: " + e.getMessage());
    }
  }

  /**
   * Tests the Dgraph connection using the ClientOptions class.
   * This test connects to a local Dgraph server with ACL credentials and plaintext.
   */
  @Test
  public void testClientOptions() {
    try {
      DgraphClient client = DgraphClient.ClientOptions.forAddress("localhost", 9180)
          .withACLCredentials("groot", "password")
          .withPlaintext()
          .build();
      assertNotNull(client);

      String query = "{ me(func: uid(1)) { uid }}";
      Response response = client.newTransaction().query(query);
      assertNotNull(response);
      assertEquals(response.getJson().toStringUtf8(), EXPECTED_UID_JSON);

      client.shutdown();
    } catch (Exception e) {
      Throwable cause = e;
      while (cause != null) {
        System.out.println("Cause: " + cause.getMessage());
        cause = cause.getCause();
      }
      fail("Failed to connect with TLS: " + e.getMessage());
    }
  }

  /**
   * Tests that connecting without ACL credentials fails with an appropriate exception.
   * The test expects an exception when trying to connect to a Dgraph server with ACL enabled
   * but without providing credentials.
   */
  @Test
  public void testOpenConnectionWithoutACL() {
    try {
      // Try to connect without providing credentials
      String conn = "dgraph://localhost:9180";
      DgraphClient client = DgraphClient.open(conn);
      
      // Try a query to trigger the authentication error
      String query = "{ me(func: uid(1)) { uid }}";
      client.newTransaction().query(query);
      
      // If we reach here, the test has failed
      fail("Expected an exception when connecting without ACL credentials");
    } catch (Exception e) {
      // The exception should contain authentication/permission related message
      // Look for exceptions like "unauthenticated", "permission denied", etc.
      boolean hasAuthError = false;
      Throwable cause = e;
      while (cause != null) {
        String message = cause.getMessage() != null ? cause.getMessage().toLowerCase() : "";
        if (message.contains("unauthenticated") || 
            message.contains("permission denied") || 
            message.contains("unauthorized") ||
            message.contains("access") ||
            message.contains("auth")) {
          hasAuthError = true;
          break;
        }
        cause = cause.getCause();
      }
      
      assertTrue(hasAuthError, "Exception should be related to authentication or permissions: " + e.getMessage());
    }
  }

  /**
   * Tests that an appropriate exception is thrown when the connection string is missing a required port.
   */
  @Test
  public void testMissingPortException() {
    try {
      // Missing port in connection string should throw IllegalArgumentException
      String conn = "dgraph://localhost";
      DgraphClient.open(conn);
      fail("Should have thrown IllegalArgumentException for missing port");
    } catch (IllegalArgumentException e) {
      // Expected exception
      assertTrue(e.getMessage().contains("port required"));
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getClass().getName() + ": " + e.getMessage());
    }
  }

  /**
   * Tests that an appropriate exception is thrown when the connection string has an invalid schema.
   */
  @Test
  public void testInvalidSchemaException() {
    try {
      // Invalid schema should throw IllegalArgumentException
      String conn = "http://localhost:9180";
      DgraphClient.open(conn);
      fail("Should have thrown IllegalArgumentException for invalid schema");
    } catch (IllegalArgumentException e) {
      // Expected exception
      assertTrue(e.getMessage().contains("scheme must be 'dgraph'"));
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getClass().getName() + ": " + e.getMessage());
    }
  }

  /**
   * Tests that an appropriate exception is thrown when the connection string includes a username but no password.
   */
  @Test
  public void testMissingPasswordException() {
    try {
      // Username without password should throw IllegalArgumentException
      String conn = "dgraph://groot@localhost:9180";
      DgraphClient.open(conn);
      fail("Should have thrown IllegalArgumentException for missing password");
    } catch (IllegalArgumentException e) {
      // Expected exception
      assertTrue(e.getMessage().contains("password required when username is provided"));
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getClass().getName() + ": " + e.getMessage());
    }
  }

  /**
   * Tests that an appropriate exception is thrown when the connection string has an invalid sslmode.
   */
  @Test
  public void testInvalidSSLModeException() {
    try {
      // Invalid sslmode should throw IllegalArgumentException
      String conn = "dgraph://localhost:9180?sslmode=invalid";
      DgraphClient.open(conn);
      fail("Should have thrown IllegalArgumentException for invalid sslmode");
    } catch (IllegalArgumentException e) {
      // Expected exception
      assertTrue(e.getMessage().contains("Invalid sslmode"));
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getClass().getName() + ": " + e.getMessage());
    }
  }

  /**
   * Tests that an appropriate exception is thrown when both apikey and bearertoken are provided.
   */
  @Test
  public void testConflictingAuthMethodsException() {
    try {
      // Both apikey and bearertoken should throw IllegalArgumentException
      String conn = "dgraph://localhost:9180?apikey=somekey&bearertoken=sometoken";
      DgraphClient.open(conn);
      fail("Should have thrown IllegalArgumentException for conflicting auth methods");
    } catch (IllegalArgumentException e) {
      // Expected exception
      assertTrue(e.getMessage().contains("apikey and bearertoken cannot both be provided"));
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getClass().getName() + ": " + e.getMessage());
    }
  }

  /**
   * Tests that authorization header is properly set when using a bearertoken.
   */
  @Test
  public void testBearerTokenAuth() {
    try {
      // This test is a bit tricky since we can't directly inspect the headers.
      // We're mostly testing that the connection string parses correctly.
      String conn = "dgraph://localhost:9180?bearertoken=test_token";
      DgraphClient client = DgraphClient.open(conn);
      assertNotNull(client);
      client.shutdown();
    } catch (Exception e) {
      fail("Failed to create client with bearertoken: " + e.getMessage());
    }
  }

  /**
   * Tests that authorization header is properly set when using an apikey.
   */
  @Test
  public void testApiKeyAuth() {
    try {
      // This test is a bit tricky since we can't directly inspect the headers.
      // We're mostly testing that the connection string parses correctly.
      String conn = "dgraph://localhost:9180?apikey=test_api_key";
      DgraphClient client = DgraphClient.open(conn);
      assertNotNull(client);
      client.shutdown();
    } catch (Exception e) {
      fail("Failed to create client with apikey: " + e.getMessage());
    }
  }
}
