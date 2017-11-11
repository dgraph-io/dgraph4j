package io.dgraph;

public class LinReadContext {

    private DgraphProto.LinRead linRead = DgraphProto.LinRead.getDefaultInstance();

    synchronized DgraphProto.LinRead getLinRead() {
      return linRead;
    }

    synchronized void setLinRead(DgraphProto.LinRead linRead) {
      this.linRead = linRead;
    }
}
