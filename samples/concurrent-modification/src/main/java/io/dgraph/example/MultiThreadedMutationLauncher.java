package io.dgraph.example;

import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;

import io.dgraph.DgraphClient;
import io.dgraph.DgraphGrpc;
import io.dgraph.DgraphGrpc.DgraphStub;
import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Operation;
import io.dgraph.Transaction;
import io.dgraph.TxnConflictException;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class MultiThreadedMutationLauncher {

	DgraphClient dgraphClient = null;

	public static void main(String[] args) {

		System.out.println("in main");

		new MultiThreadedMutationLauncher().doProcess();

	}

	public MultiThreadedMutationLauncher() {
		//
		ManagedChannel channel1 = ManagedChannelBuilder.forAddress("localhost", 9080).usePlaintext().build();
		DgraphStub stub1 = DgraphGrpc.newStub(channel1);
		//
		dgraphClient = new DgraphClient(stub1);
	}

	private void doProcess() {
		dropAll();
		createSchema();
		doSetupTransaction();
		//
		doQueryAndMutation();

	}

	private void doQueryAndMutation() {
		//collect mutations
		List<MultiThreadedMutation> mutations=new ArrayList<MultiThreadedMutation>();
		for(int i=0;i<2;i++) {
			MultiThreadedMutation mtMutation=new MultiThreadedMutation(dgraphClient);
			mutations.add(mtMutation);
		}
		
		//launch threads		
		for(MultiThreadedMutation mutation:mutations) {
			Thread t=new Thread(mutation);
			t.start();
		}
		
	}

	private void doSetupTransaction() {
		Transaction txn = dgraphClient.newTransaction();
		Gson gson = new Gson();
		try {
			// Create data
			Person personAlice = new Person();
			personAlice.name = "Alice";
			personAlice.clickCount = 1;
			//
			Person personJohn = new Person();
			personJohn.name = "John";
			personJohn.clickCount = 1;

			String json = gson.toJson(personAlice);
			Mutation mu = Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(json.toString())).build();		
			txn.mutate(mu);
			
			json = gson.toJson(personJohn);
			Mutation mu1 = Mutation.newBuilder().setSetJson(ByteString.copyFromUtf8(json.toString())).build();		
			txn.mutate(mu1);
			
			txn.commit();
			
		} catch (TxnConflictException ex) {
			System.out.println(ex);
		} finally {
			// Clean up. Calling this after txn.commit() is a no-op
			// and hence safe.
			txn.discard();
		}


	}
	
	private void createSchema() {
		String schema = "name: string @index(exact) .\n " + "email: string @index(exact) .\n"
	            + "clickCount: int  .\n"; 
				
		Operation operation = Operation.newBuilder().setSchema(schema).setRunInBackground(true).build();
		dgraphClient.alter(operation);
	}

	private void dropAll() {
		dgraphClient.alter(Operation.newBuilder().setDropAll(true).build());
		System.out.println("existing schema dropped");
	}

}
