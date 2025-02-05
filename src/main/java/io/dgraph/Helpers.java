/*
 * SPDX-FileCopyrightText: © Hypermode Inc. <hello@hypermode.com>
 * SPDX-License-Identifier: Apache-2.0
 */

package io.dgraph;

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
}
