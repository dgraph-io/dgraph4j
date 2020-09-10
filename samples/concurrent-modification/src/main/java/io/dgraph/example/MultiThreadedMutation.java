package io.dgraph.example;

import io.dgraph.DgraphProto.Mutation;
import io.dgraph.DgraphProto.Request;
import com.google.gson.Gson;
import io.dgraph.DgraphProto.Response;
import com.google.protobuf.ByteString;
import io.dgraph.DgraphClient;
import io.dgraph.Transaction;
import io.dgraph.TxnConflictException;
import java.util.Collections;
import java.util.Map;

public class MultiThreadedMutation implements Runnable {
	static Integer globalThreadNumberCount = 1;
	int threadNumber = 0;
	private DgraphClient dgraphClient;
	private Transaction txn;
	static final int MAX_RETRY_COUNT = 5;

	public MultiThreadedMutation(DgraphClient dgraphClient) {
		synchronized (globalThreadNumberCount) {
			this.threadNumber = globalThreadNumberCount++;
			this.dgraphClient = dgraphClient;
		}

	}

	public void run() {
		boolean successFlag = false;
		int retryCount = 0;
		while (retryCount < MAX_RETRY_COUNT) {
			try {
				doMutation();
				successFlag = true;
				System.out.println(System.currentTimeMillis() + " Thread #" + threadNumber + " succeeded after "
						+ retryCount + " retries");
				break;
			} catch (TxnConflictException txnConflictException) {
				try {
					System.out.println(System.currentTimeMillis() + " Thread #" + threadNumber
							+ " found a concurrent modification conflict, sleeping for 1 second...");
					Thread.sleep(1000);
					System.out.println(System.currentTimeMillis() + " Thread #" + threadNumber + " resuming");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				retryCount++;
			} catch (Exception e) {
				// No retryable exception
				e.printStackTrace();
				break;
			}
		}
		if (!successFlag && retryCount >= MAX_RETRY_COUNT) {
			System.out.println(System.currentTimeMillis() + " Thread #" + threadNumber + " giving up transaction after "
					+ (retryCount - 1) + " retries");
		}
	}

	private void doMutation() throws Exception {
		txn = dgraphClient.newTransaction();
		Gson gson = new Gson();
		// Query
		String query = "query all($a: string){\n" + "  all(func: eq(name, $a)) {\n" + "    " + "uid\n" + "name\n"
				+ "clickCount\n" + "  }\n" + "}\n";

		Map<String, String> vars = Collections.singletonMap("$a", "Alice");

		Response response = dgraphClient.newReadOnlyTransaction().queryWithVars(query, vars);

		// Deserialize
		People ppl = gson.fromJson(response.getJson().toStringUtf8(), People.class);
		for (Person person : ppl.all) {
			System.out.println(System.currentTimeMillis() + " Thread #" + threadNumber
					+ " increasing clickCount for uid" + person.uid + " ,Name: " + person.name);
			// do mutation
			person.clickCount = person.clickCount + 1;

			try {

				String upsertQuery = "query {\n" + "user as var(func: eq(name, \"" + person.name + "\"))\n" + "}\n";

				Mutation mu2 = Mutation.newBuilder()
						.setSetNquads(ByteString.copyFromUtf8("uid(user) <clickCount> \"" + person.clickCount + "\" ."))
						.build();

//				System.out.println(mu2.toString() + " for key " + person.name);
				Request request = Request.newBuilder().setQuery(upsertQuery).addMutations(mu2).setCommitNow(true)
						.build();
				txn.doRequest(request);
				txn.close();
			} catch (Exception ex) {
				// if its a conflict exception, we can retry
				if (ex.getCause().getCause() instanceof TxnConflictException) {
					TxnConflictException txnConflictException = (TxnConflictException) ex.getCause().getCause();
					txn.close();
					throw (txnConflictException);
				} else {
					throw ex;
				}

			} finally {
				txn.discard();
			}

		}
	}
}
