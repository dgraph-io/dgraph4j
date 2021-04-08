package io.dgraph;

import static io.dgraph.DgraphIntegrationTest.TEST_HOSTNAME;
import static io.dgraph.DgraphIntegrationTest.TEST_HTTP_PORT;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtil {
  private static final String GROOT_USERNAME = "groot";
  private static final String GROOT_PASSWORD = "password";
  private static final String ADMIN_HTTP_ENDPOINT =
      "http://" + TEST_HOSTNAME + ":" + TEST_HTTP_PORT + "/admin";

  private static class GraphQLRequest {
    public String query;
    public Map<String, Object> variables;

    // excluded from serialization/deserialization
    public transient Map<String, String> headers;

    public GraphQLResponse execute(String url) throws Exception {
      Gson gson = new Gson();
      String response = executePost(url, gson.toJson(this), this.headers);
      return gson.fromJson(response, GraphQLResponse.class);
    }

    public GraphQLResponse executeAsGroot(String url) throws Exception {
      if (this.headers == null) {
        this.headers = new HashMap<>();
      }
      this.headers.put("X-Dgraph-AccessToken", login(GROOT_USERNAME, GROOT_PASSWORD));
      return this.execute(url);
    }
  }

  private static class GraphQLResponse {
    public List<GqlError> errors;
    public Map<String, Object> data;

    public static class GqlError {
      public String message;
    }

    public void assertNoError() throws Exception {
      if (this.errors != null && this.errors.size() > 0) {
        throw new Exception(this.errors.toString());
      }
    }
  }

  /** @return the accessJWT of the logged-in user. */
  public static String login(String userId, String password) throws Exception {
    GraphQLRequest req = new GraphQLRequest();
    req.query =
        ""
            + "mutation login($userId: String, $pass: String) {\n"
            + "   login(userId: $userId, password: $pass) {\n"
            + "     response {\n"
            + "       accessJWT\n"
            + "     }\n"
            + "   }\n"
            + "}";
    req.variables =
        new HashMap<String, Object>() {
          {
            put("userId", userId);
            put("pass", password);
          }
        };

    GraphQLResponse resp = req.execute(ADMIN_HTTP_ENDPOINT);
    resp.assertNoError();

    return (String)
        ((Map<String, Object>) ((Map<String, Object>) resp.data.get("login")).get("response"))
            .get("accessJWT");
  }

  public static void addUser(String username, String password) throws Exception {
    GraphQLRequest req = new GraphQLRequest();
    req.query =
        ""
            + "mutation addUser($name: String!, $pass: String!) {\n"
            + "   addUser(input: [{name: $name, password: $pass}]) {\n"
            + "     user {\n"
            + "       name\n"
            + "     }\n"
            + "   }\n"
            + "}";
    req.variables =
        new HashMap<String, Object>() {
          {
            put("name", username);
            put("pass", password);
          }
        };

    GraphQLResponse resp = req.executeAsGroot(ADMIN_HTTP_ENDPOINT);
    resp.assertNoError();
  }

  /** @param setOrRemove true if set, false if remove. */
  public static void updateUser(String username, String group, boolean setOrRemove)
      throws Exception {
    GraphQLRequest req = new GraphQLRequest();
    if (setOrRemove) {
      req.query =
          ""
              + "mutation updateUser($name: String, $group: String!) {\n"
              + "   updateUser(input: {filter: {name: {eq: $name}}, set: {groups: [{name: $group}]}}) {\n"
              + "     user {\n"
              + "       name\n"
              + "     }\n"
              + "   }\n"
              + "}";
    } else {
      req.query =
          ""
              + "mutation updateUser($name: String, $group: String!) {\n"
              + "   updateUser(input: {filter: {name: {eq: $name}}, remove: {groups: [{name: $group}]}}) {\n"
              + "     user {\n"
              + "       name\n"
              + "     }\n"
              + "   }\n"
              + "}";
    }
    req.variables =
        new HashMap<String, Object>() {
          {
            put("name", username);
            put("group", group);
          }
        };

    GraphQLResponse resp = req.executeAsGroot(ADMIN_HTTP_ENDPOINT);
    resp.assertNoError();
  }

  public static void deleteUser(String username) throws Exception {
    GraphQLRequest req = new GraphQLRequest();
    req.query =
        ""
            + "mutation ($name: String!) {\n"
            + "   deleteUser(filter: {name: { eq: $name } }) {\n"
            + "     msg\n"
            + "     numUids\n"
            + "   }\n"
            + "}";
    req.variables =
        new HashMap<String, Object>() {
          {
            put("name", username);
          }
        };

    GraphQLResponse resp = req.executeAsGroot(ADMIN_HTTP_ENDPOINT);
    resp.assertNoError();
  }

  public static void addGroup(String groupname) throws Exception {
    GraphQLRequest req = new GraphQLRequest();
    req.query =
        ""
            + "mutation addGroup($name: String!) {\n"
            + "   addGroup(input: [{name: $name}]) {\n"
            + "     group {\n"
            + "       name\n"
            + "     }\n"
            + "   }\n"
            + "}";
    req.variables =
        new HashMap<String, Object>() {
          {
            put("name", groupname);
          }
        };

    GraphQLResponse resp = req.executeAsGroot(ADMIN_HTTP_ENDPOINT);
    resp.assertNoError();
  }

  public static void updateGroup(String group, String pred, int perm) throws Exception {
    GraphQLRequest req = new GraphQLRequest();
    req.query =
        ""
            + "mutation updateGroup($gname: String!, $pred: String!, $perm: Int!) {\n"
            + "   updateGroup(input: {filter: {name: {eq: $gname}}, set: {rules: [{predicate: $pred, permission: $perm}]}}) {\n"
            + "     group {\n"
            + "       name\n"
            + "     }\n"
            + "   }\n"
            + "}";
    req.variables =
        new HashMap<String, Object>() {
          {
            put("gname", group);
            put("pred", pred);
            put("perm", perm);
          }
        };

    GraphQLResponse resp = req.executeAsGroot(ADMIN_HTTP_ENDPOINT);
    resp.assertNoError();
  }

  private static String executePost(String targetURL, String body, Map<String, String> headers)
      throws Exception {
    HttpURLConnection connection = null;

    try {
      // Create connection
      URL url = new URL(targetURL);
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      if (headers != null) {
        for (Map.Entry<String, String> entry : headers.entrySet()) {
          connection.setRequestProperty(entry.getKey(), entry.getValue());
        }
      }
      connection.setRequestProperty("Content-Type", "application/json");
      connection.setRequestProperty("Content-Length", Integer.toString(body.getBytes().length));

      connection.setUseCaches(false);
      connection.setDoOutput(true);

      // Send request
      DataOutputStream wr = new DataOutputStream(connection.getOutputStream());
      wr.writeBytes(body);
      wr.close();

      // Get Response
      InputStream is = connection.getInputStream();
      BufferedReader rd = new BufferedReader(new InputStreamReader(is));
      StringBuilder response = new StringBuilder();
      String line;
      while ((line = rd.readLine()) != null) {
        response.append(line);
      }
      rd.close();
      return response.toString();
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }
}
