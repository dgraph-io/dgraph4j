/*
 * Copyright (C) 2017-18 Dgraph Labs, Inc. and Contributors
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

import java.util.Map;

public class Helpers {
  /**
   * Sets the edges corresponding to predicates on the node with the given uid for deletion. This
   * function returns a new Mutation object with the edges set. It is the caller's responsibility to
   * run the mutation by calling {@code AsyncTransaction#mutate}.
   *
   * @param mu Mutation to add edges to
   * @param uid uid of the node
   * @param predicates predicates of the edges to remove
   * @return a new Mutation object with the edges set
   */
  public static DgraphProto.Mutation deleteEdges(
      DgraphProto.Mutation mu, String uid, String... predicates) {
    DgraphProto.Mutation.Builder b = DgraphProto.Mutation.newBuilder(mu);
    for (String predicate : predicates) {
      b.addDel(
          DgraphProto.NQuad.newBuilder()
              .setSubject(uid)
              .setPredicate(predicate)
              .setObjectValue(DgraphProto.Value.newBuilder().setDefaultVal("_STAR_ALL").build())
              .build());
    }
    return b.build();
  }

  /**
   * Merges source LinRead instance contents to the destination instance if the version in the
   * source is greater than the one in the destination.
   *
   * @param src - the source LinRead instance
   * @param dst - the destination LinRead instance
   * @return - new merged LinRead instance
   */
  public static DgraphProto.LinRead mergeLinReads(
      DgraphProto.LinRead dst, DgraphProto.LinRead src) {
    DgraphProto.LinRead.Builder result = DgraphProto.LinRead.newBuilder(dst);
    Map<Integer, Long> dstMap = dst.getIdsMap();

    src.getIdsMap()
        .entrySet()
        .stream()
        .filter(
            (Map.Entry<Integer, Long> e) -> {
              Long dstValue = dstMap.get(e.getKey());
              return dstValue == null || dstValue < e.getValue();
            })
        .forEach(
            (Map.Entry<Integer, Long> e) -> {
              result.putIds(e.getKey(), e.getValue());
            });

    return result.build();
  }
}
