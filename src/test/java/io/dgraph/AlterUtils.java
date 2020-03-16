/*
 * Copyright (C) 2018 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.dgraph;

import static org.testng.Assert.fail;

import com.google.gson.Gson;
import io.dgraph.DgraphProto.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AlterUtils {
  private static Gson gson = new Gson();

  public static void waitForIndexing(
      DgraphClient dgraphClient, String pred, List<String> toks, boolean count, boolean reverse) {
    String query = String.format("schema(pred: [%s]){tokenizer reverse count}", pred);
    Collections.sort(toks);

    while (true) {
      Response resp = dgraphClient.newReadOnlyTransaction().query(query);
      if (hasIndexes(resp, toks, count, reverse)) {
        break;
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        fail("unable to wait for indexing");
      }
    }
  }

  public static void waitForIndexing(
      DgraphAsyncClient asyncClient,
      String pred,
      List<String> toks,
      boolean count,
      boolean reverse) {
    String query = String.format("schema(pred: [%s]){tokenizer reverse count}", pred);
    Collections.sort(toks);

    while (true) {
      Response resp = asyncClient.newReadOnlyTransaction().query(query).join();
      if (hasIndexes(resp, toks, count, reverse)) {
        break;
      }

      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        fail("unable to wait for indexing");
      }
    }
  }

  private static boolean hasIndexes(
      Response resp, List<String> toks, boolean count, boolean reverse) {
    Schema schema = gson.fromJson(resp.getJson().toStringUtf8(), Schema.class);
    if (schema.schema.size() != 1) {
      return false;
    }

    Index index = schema.schema.get(0);
    if (index.count != count || index.reverse != reverse || index.tokenizer.size() != toks.size()) {
      return false;
    }

    Collections.sort(index.tokenizer);
    for (int i = 0; i < toks.size(); i++) {
      if (!toks.get(i).equals(index.tokenizer.get(i))) {
        return false;
      }
    }

    return true;
  }

  static class Index {
    ArrayList<String> tokenizer = new ArrayList<>();
    boolean count = false;
    boolean reverse = false;
  }

  static class Schema {
    List<Index> schema;
  }
}
