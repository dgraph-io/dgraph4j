package io.dgraph.entity;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;

import io.dgraph.exception.DGraphException;
import io.dgraph.proto.Mutation;
import io.dgraph.proto.Request;
import lombok.Getter;
import lombok.Setter;
import protos.Schema;
import protos.Schema.SchemaUpdate;

@Getter
@Setter
public class DgraphRequest {

    private Request gr;

    private Integer line;

    private Gson gson;

    public DgraphRequest() {
        super();
        this.gson = new Gson();
    }

    public Request Request() {
        return gr;
    }

    public void setQuery(String s) {
        this.gr = gr.toBuilder().setQuery(s).build();
    }

    public void setSchema(String s) {
        String query = String.format("mutation {\nschema {\n%s\n}\n}", s);
        this.setQuery(query);
    }

    public void setQueryWithVariables(String query, Map<String, String> varData) {
        this.gr = gr.toBuilder().setQuery(query).build();
    }

    public void addMutation(Edge e, String opType) {

        initMutation();
        Mutation currMut = gr.getMutation();

        if (opType.equalsIgnoreCase("SET")) {
            gr = gr.toBuilder().setMutation(currMut.toBuilder().addSet(e.nq).build()).build();
        } else {
            gr = gr.toBuilder().setMutation(currMut.toBuilder().addDel(e.nq).build()).build();
        }

    }

    public void set(Edge e) {
        e.validate();
        this.addMutation(e, CommonConstants.OPTYPE_SET);
    }

    public void del(Edge e) {
        e.validate();
        this.addMutation(e, CommonConstants.OPTYPE_DEL);
    }

    public void setObject(Object obj) {

        if (obj == null) {
            throw new DGraphException("Object to be created cant be null");
        }

        String objJson = gson.toJson(obj);

        if (StringUtils.isBlank(objJson)) {
            throw new DGraphException("Unable to marshall to json");
        }
        initMutation();
        this.gr = gr.toBuilder().setMutation(gr.getMutation().toBuilder().setSetJson(ByteString.copyFrom(objJson.getBytes())).build()).build();

    }

    public void deleteObject(Object obj) {

        if (obj == null) {
            throw new DGraphException("Object to be created cant be null");
        }

        String objJson = gson.toJson(obj);

        if (StringUtils.isBlank(objJson)) {
            throw new DGraphException("Unable to marshall to json");
        }
        initMutation();
        this.gr = gr.toBuilder().setMutation(gr.getMutation().toBuilder().setDeleteJson(ByteString.copyFrom(objJson.getBytes())).build()).build();

    }

    private void initMutation() {

        if (gr == null || this.gr.getMutation() == null) {
            this.gr = Request.newBuilder().setMutation(Mutation.newBuilder().build()).build();
        }

    }

    public void addSchema(SchemaUpdate sch) {
        initMutation();
        this.gr.getMutation().getSchemaList().add(sch);
    }

    public int size() {
        if (this.gr.getMutation() == null) {
            return 0;
        }
        return this.gr.getMutation().getDelCount() + this.gr.getMutation().getSetCount() + this.gr.getMutation().getSchemaCount();
    }

    public void reset() {
        this.gr = Request.newBuilder().build();
    }

}
