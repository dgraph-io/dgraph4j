package io.dgraph.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class Node {
    
    Long uid;
    
    private String varName;

    public Node(String varName) {
       this.setVarName(varName);
    }
    
    public String String() {
        
        if(uid != null) {
            return Long.toString(uid);
        }
        
        return varName;
    }
    
    
    public Edge connectTo(String pred, Node conn) {
        Edge e = new Edge();
        e.setSubject(conn);
        e.nq = e.nq.toBuilder().setPredicate(pred).build();
        e.connectTo(conn);
        return e;
    }

    
    public Edge Edge(String pred) {
        Edge e = new Edge();
        e.setSubject(this);
        e.nq = e.nq.toBuilder().setPredicate(pred).build();
        return e;
    }
    
    public Edge Delete() {
        
        //TODO
        return null;
        
    }
}
