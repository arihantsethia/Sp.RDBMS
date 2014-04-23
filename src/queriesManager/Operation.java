package queriesManager;

import queriesManager.QueryParser.OperationType;

/**
 * 
 * The instance of "Operation" class is called whenever we want to execute
 * any query. 
 * It checks operationType and create class instance according to it.
 * 
 */
public abstract class Operation {
	protected OperationType operationType;
	protected Condition condition;
	protected Projection projection;

	/**
	 * set Type of operation.
	 * @param opType
	 */
	void setType(OperationType opType) {
		operationType = opType;
	}

	/**
	 * constructor to create operation instance corresponding to operation type.
	 * @param statement
	 * @return
	 */
	public static Operation makeOperation(String statement) {
		if (statement.toUpperCase().contains("NATURAL")) {
			return new NaturalJoin(statement);
		} else if (statement.toUpperCase().contains("EQUI")) {
			return new EquiJoin(statement);
		} else if (statement.toUpperCase().contains("JOIN")) {
			return new CondJoin(statement);
		} else if (statement.toUpperCase().contains("SELECT")) {
			return new SelectOperation(statement);
		} else if (statement.toUpperCase().contains("UPDATE")) {
			return new UpdateOperation(statement);
		} else if (statement.toUpperCase().contains("INSERT")) {
			return new InsertOperation(statement);
		} else if (statement.toUpperCase().contains("CREATE")) {
			return new CreateOperation(statement);
		} else if (statement.toUpperCase().contains("DROP")) {
			return new DropOperation(statement);
		} else if (statement.toUpperCase().contains("ALTER")) {
			return new AlterOperation(statement);
		} else if (statement.toUpperCase().contains("DELETE")) {
			return new DeleteOperation(statement);
		}
		return null;
	}

	/**
	 * it is overloaded by derived classes to set condition class instance corresponding to where statement.
	 * @param cond
	 */
	void setCondition(Condition cond) {
		condition = cond;
	}

	/**
	 * it is overloaded by derived classes to evaluate corresponding statement.
	 * @return
	 */
	public boolean executeOperation() {
		return false;
	}
}
