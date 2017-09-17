package io.dgraph.entity;

import java.util.Map;

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
        this.gr = gr.toBuilder().setQuery(query) .build();

        // TODO
    }

    public void addMutation(Edge e, String opType) {

        if (gr == null || this.gr.getMutation() == null) {
            this.gr = Request.newBuilder().setMutation(Mutation.newBuilder().build()).build();
        }

        if (opType.equalsIgnoreCase("SET")) {
            Mutation currMut = gr.getMutation() ;
            gr = gr.toBuilder().setMutation(currMut.toBuilder().addSet(e.nq).build()).build();
            
   //         gr.getMutation().getSetList().add(e.nq);
        } else {
            gr.getMutation().getDelList().add(e.nq);
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

    public void addSchema(SchemaUpdate sch) {
        if (this.gr.getMutation() == null) {
            this.gr = gr.toBuilder().setMutation(Mutation.newBuilder().build()).build();
        }

        this.gr.getMutation().getSchemaList().add(sch);
    }
    
    public int size() {
        if(this.gr.getMutation() == null) {
            return 0;
        }
        return this.gr.getMutation().getDelCount() + this.gr.getMutation().getSetCount() + this.gr.getMutation().getSchemaCount();
    }

    public void reset() {
        this.gr = Request.newBuilder().build();
    }
    


}
