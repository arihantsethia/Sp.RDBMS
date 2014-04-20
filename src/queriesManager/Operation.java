package queriesManager;

import queriesManager.QueryParser.OperationType;

public abstract class Operation {
	protected OperationType operationType;
	protected Condition condition;
	protected Projection projection;

	void setType(OperationType opType) {
		operationType = opType;
	}

	public static Operation makeOperation(String statement) {
		if (statement.toUpperCase().contains("SELECT")) {
			return new SelectOperation(statement);
		} else if (statement.toUpperCase().contains("UPDATE")) {
			return new UpdateOperation(statement);
		} else if (statement.toUpperCase().contains("INSERT")) {
			return new InsertOperation(statement);
		}
		return null;
	}

	void setCondition(Condition cond) {
		condition = cond;
	}

	public boolean executeOperation() {
		return false;
	}
}
