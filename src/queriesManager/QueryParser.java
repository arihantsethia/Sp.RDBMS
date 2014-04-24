package queriesManager;

import java.util.Vector;
import java.util.TreeMap;

import databaseManager.Attribute;
import databaseManager.ObjectHolder;
import databaseManager.Relation;
import databaseManager.Utility;
import databaseManager.Attribute.Type;

public class QueryParser {
	public static Attribute attributeName;
	public static TreeMap<String, String> tableMap;

	public static enum OperationType {
		NATURALJOIN, CONDJOIN, EQUIJOIN, SELECT, UPDATE, CREATE, DROP, DELETE;
		public static String toString(OperationType opType) {
			if (opType == QueryParser.OperationType.NATURALJOIN) {
				return "NATURALJOIN";
			} else if (opType == QueryParser.OperationType.EQUIJOIN) {
				return "EQUIJOIN";
			} else if (opType == QueryParser.OperationType.SELECT) {
				return "SELECT";
			} else if (opType == QueryParser.OperationType.UPDATE) {
				return "UPDATE";
			} else if (opType == QueryParser.OperationType.CREATE) {
				return "CREATE";
			} else if (opType == QueryParser.OperationType.DROP) {
				return "DROP";
			} else if (opType == QueryParser.OperationType.DELETE) {
				return "DELETE";
			} else {
				return "ALTER";
			}
		}
	};

	public static enum ConditionType {
		OR, AND, SIMPLE, NULL;
		public static String toString(ConditionType cndType) {
			if (cndType == QueryParser.ConditionType.OR) {
				return "OR";
			} else if (cndType == QueryParser.ConditionType.AND) {
				return "AND";
			} else {
				return "SIMPLE";
			}
		}
	};

	public static enum OperatorType {
		LESS, GREATER, LESSEQUAL, GREATEQUAL, EQUAL;
		public static String toString(OperatorType opType) {
			if (opType == QueryParser.OperatorType.LESS) {
				return "<";
			} else if (opType == QueryParser.OperatorType.GREATER) {
				return ">";
			} else if (opType == QueryParser.OperatorType.LESSEQUAL) {
				return "<=";
			} else if (opType == QueryParser.OperatorType.GREATEQUAL) {
				return ">=";
			} else {
				return "=";
			}
		}
	};

	public static enum DataType {
		Int, Long, Boolean, Float, Double, Char, Undeclared;
		public static Type toType(int value) {
			return Type.values()[value];
		}

		public static int toInt(Type value) {
			return value.ordinal();
		}

	};

	public QueryParser() {
		tableMap = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
	}

	static Vector<String> getSelectTableList(String statement) {
		Vector<String> result;
		statement = statement.trim();
		String[] tableList = statement.split(",");
		result = new Vector<String>(tableList.length);
		for (int i = 0; i < tableList.length; i++) {
			result.add(i, tableList[i].trim());
		}
		return result;
	}

	static Vector<String> statementParts(String statement, String opcode) {
		Vector<String> result = new Vector<String>();
		int index = statement.indexOf(opcode);
		statement = statement.substring(index + opcode.length()).trim();
		index = statement.indexOf("from");
		if (index != -1) {
			result.addElement(statement.substring(0, index).trim());
			String restPart = statement.substring(index + 4);
			index = restPart.indexOf("where");
			if (index != -1) {
				result.addElement(restPart.substring(0, index).trim());
				result.addElement(restPart.substring(index + 5, restPart.length()).trim());
			} else {
				result.addElement(restPart.trim());
			}
			return result;
		}
		return null;
	}

	public static boolean isInsertStatementQuery(String statement) {
		int insertIndex = statement.trim().indexOf("insert");
		int intoIndex = statement.trim().indexOf("into");
		int valueIndex = statement.trim().indexOf("values");

		String relationName = statement.substring(intoIndex + 4, statement.indexOf("(")).trim();
		long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);

		if (insertIndex == 0 && intoIndex != -1) {
			if (newRelationId != -1) {
				String columnPart = "";
				columnPart = statement.substring(statement.indexOf("(") + 1, valueIndex).trim();
				columnPart = columnPart.replace(")", " ").trim();
				String[] columnPartSplit = columnPart.split(",");

				if (valueIndex != -1) {
					String valuePart = "";
					valuePart = statement.substring(valueIndex + 6).trim();
					valuePart = valuePart.replace("(", " ").replace(")", " ").trim();
					String[] valuePartSplit = valuePart.split(",");

					if (columnPartSplit.length == valuePartSplit.length) {
						Relation newRelation = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId);
						Vector<Attribute> attributes = newRelation.getAttributes();

						for (int i = 0; i < columnPartSplit.length; i++) {
							boolean chk = true;
							for (int k = 0; k < attributes.size(); k++) {
								if (attributes.get(k).getName().equals(columnPartSplit[i].trim())) {
									Attribute.Type fieldType = newRelation.getAttributeType(columnPartSplit[i].trim());

									if (!Utility.isSameType(fieldType, valuePartSplit[i].trim())) {
										print_error(5, fieldType + " " + valuePartSplit[i].trim());
										return false;
									}
									chk = false;
								}
							}
							if (chk) {
								print_error(8, columnPartSplit[i].trim());
								return false;
							}
						}
					} else {
						System.out.println("Error: #columns and #values mismatch");
						return false;
					}
				} else {
					System.out.println("Error: Keyword \'values\' is missing");
					return false;
				}
			} else {
				System.out.println("Error: "+relationName + " is not a valid Relation Name");
				return false;
			}
		} else {
			System.out.println("Error: Not a valid Insert syntax");
			return false;
		}

		return true;
	}

	public static boolean isDeleteStatementQuery(String statement) {
		int deleteIndex = statement.trim().indexOf("delete");
		int fromIndex = statement.trim().indexOf("from");
		tableMap = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);
		if (deleteIndex == 0 && fromIndex != -1) {
			String fromPart = "";
			int whereIndex = statement.trim().indexOf("where");

			if (whereIndex != -1) {
				String wherePart = "";
				fromPart = statement.substring(fromIndex + 4, whereIndex).trim();
				wherePart = statement.substring(whereIndex + 5).trim();

				String[] fromPartSplit = fromPart.split(",");

				tableMap.clear();

				for (int i = 0; i < fromPartSplit.length; i++) {
					tableMap.put(Utility.getNickName(fromPartSplit[i]), Utility.getRelationName(fromPartSplit[i]));
				}

				boolean whereClause = isCondition(wherePart);

				if (whereClause) {
					return true;
				} else {
					return false;
				}
			} else {
				String relationName = statement.substring(fromIndex + 4).trim();
				long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);
				if (newRelationId != -1) {
					return true;
				} else {
					print_error(2, relationName);
					return false;
				}
			}
		}
		return true;
	}

	public static boolean isUpdateStatementQuery(String statement) {
		int updateIndex = statement.trim().indexOf("update");
		int setIndex = statement.trim().indexOf("set");
		tableMap = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);

		if (updateIndex == 0 && setIndex != -1) {
			String updatePart = "", setPart = "";
			updatePart = statement.substring(updateIndex + 6, setIndex).trim();
			int whereIndex = statement.trim().indexOf("where");

			String[] updatePartSplit = updatePart.split(",");
			String key = "", field = "";
			String relationName = "";

			if (whereIndex != -1) {
				String wherePart = "";
				setPart = statement.substring(setIndex + 3, whereIndex).trim();
				wherePart = statement.substring(whereIndex + 5).trim();
				String[] setPartSplit = setPart.split(",");

				tableMap.clear();

				for (int i = 0; i < updatePartSplit.length; i++) {
					if (updatePartSplit[i].contains("as")) {
						tableMap.put(Utility.getNickName(updatePartSplit[i]), Utility.getRelationName(updatePartSplit[i]));
					} else {
						print_error(10, "");
						return false;
					}
				}

				for (int j = 0; j < setPartSplit.length; j++) {
					String leftPart = "", rightPart = "";
					leftPart = setPartSplit[j].substring(0, setPartSplit[j].indexOf("=")).trim();
					rightPart = setPartSplit[j].substring(setPartSplit[j].indexOf("=") + 1).trim();
					if (!leftPart.contains(".")) {
						print_error(11, "");
						return false;
					}
					key = Utility.getRelationName(leftPart);
					if (tableMap.containsKey(key)) {
						field = Utility.getNickName(leftPart);
						relationName = tableMap.get(key);
						long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);
						if (newRelationId != -1) {
							Relation newRelation = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId);
							Vector<Attribute> attributes = newRelation.getAttributes();
							boolean chk = true;
							for (int k = 0; k < attributes.size(); k++) {
								if (attributes.get(k).getName().equals(field)) {
									Attribute.Type fieldType = newRelation.getAttributeType(field);
									if (!Utility.isSameType(fieldType, rightPart)) {
										return false;
									}
									chk = false;
								}
							}
							if (chk) {
								print_error(1, field);
								return false;
							}

						} else {
							print_error(2, relationName);
							return false;
						}
					} else {
						print_error(3, key);
						return false;
					}
				}

				boolean whereClause = isCondition(wherePart);

				if (whereClause) {
					return true;
				} else {
					return false;
				}
			} else {
				setPart = statement.substring(setIndex + 3).trim();
				String[] setPartSplit = setPart.split(",");

				tableMap.clear();

				for (int i = 0; i < updatePartSplit.length; i++) {
					tableMap.put(Utility.getNickName(updatePartSplit[i]), Utility.getRelationName(updatePartSplit[i]));
				}

				for (int j = 0; j < setPartSplit.length; j++) {
					String leftPart = "", rightPart = "";
					leftPart = setPartSplit[j].substring(0, setPartSplit[j].indexOf("=")).trim();
					rightPart = setPartSplit[j].substring(setPartSplit[j].indexOf("=") + 1).trim();
					if (!leftPart.contains(".")) {
						print_error(11, "");
						return false;
					}
					key = Utility.getRelationName(leftPart);
					if (tableMap.containsKey(key)) {
						field = Utility.getNickName(leftPart);
						relationName = tableMap.get(key);
						long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);

						if (newRelationId != -1) {
							Relation newRelation = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId);
							Vector<Attribute> attributes = newRelation.getAttributes();

							boolean chk = true;
							for (int k = 0; k < attributes.size(); k++) {

								if (attributes.get(k).getName().equals(field)) {
									Attribute.Type fieldType = newRelation.getAttributeType(field);
									if (!Utility.isSameType(fieldType, rightPart)) {
										return false;
									}
									chk = false;
								}
							}
							if (chk) {
								print_error(1, field);
								return false;
							}
						} else {
							print_error(3, relationName);
							return false;
						}
					} else {
						print_error(3, key);
						return false;
					}
				}
			}
		} else {
			print_error(7, "");
			return false;
		}
		return true;
	}

	public static boolean isNaturalJoinQuery(String statement){
		int naturalJoinIndex = statement.indexOf(" naturaljoin ");
		if(naturalJoinIndex != -1){
			statement = statement.replaceAll(" naturaljoin "," , ");
			if(isSelectStatementQuery(statement)){
				return true;
			}
			else{
				System.out.println("Error: Not a valid natural join syntax");
				return false;
			}
		}
		else{
			System.out.println("Error: Not a valid natural join syntax: keyword \'naturaljoin\' is missing");
			return false;
		}
	}
	
	public static boolean isEquiJoinQuery(String statement){
		int equiJoinIndex = statement.indexOf(" equijoin ");
		if(equiJoinIndex != -1){
			statement = statement.replaceAll(" equijoin ", " , ").trim();
			if(isSelectStatementQuery(statement)){
				return true;
			}
			else{
				System.out.println("Error: Not a valid equi join syntax");
				return false;
			}
		}
		else{
			System.out.println("Error: Not a valid equi join syntax: keyword \'equijoin\' is missing");
			return false;
		}
	}
	
	public static boolean isConditionalJoinQuery(String statement){
		int joinIndex = statement.indexOf(" join ");
		if(joinIndex != -1){
			int whereIndex = statement.indexOf("where");
			if(whereIndex != -1){
				int lp = statement.indexOf("(");
				int rp = statement.indexOf(")");
				
				if(lp < rp && rp < whereIndex){
					statement = statement.replace(" join ", " , ") ;
					statement = statement.replaceFirst("("," ").replaceFirst(")"," ").trim();
					if(isSelectStatementQuery(statement)){
						return true;
					}
					else{
						System.out.println("Error: Not a valid join syntax");
						return false;
					}
				}
				else{
					System.out.println("Error: Improper positioning of parenthesis");
					return false;
				}
			}
			else{
				statement = statement.replace(" join ", ",").trim();
				statement = statement.replace("("," ").replace(")"," ").trim();
				
				if(isSelectStatementQuery(statement)){
					return true;
				}
				else{
					System.out.println("Error: Not a valid join syntax");
					return false;
				}
			}
		}
		else{
			System.out.println("Error: Not a valid join syntax: keyword \'join\' is missing");
			return false;
		}
	}
	
	public static boolean isSelectStatementQuery(String statement) {
		int selectIndex = statement.trim().indexOf("select");
		int fromIndex = statement.trim().indexOf("from");

		tableMap = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER);

		if (selectIndex == 0 && fromIndex != -1) {
			String selectPart = "", fromPart = "";
			selectPart = statement.trim().substring(selectIndex + 6, fromIndex).trim();
			int whereIndex = statement.trim().indexOf("where");

			String[] selectPartSplit = selectPart.split(",");
			String key = "", field = "";
			String relationName = "";

			if (whereIndex != -1) {
				String wherePart = "";
				fromPart = statement.trim().substring(fromIndex + 4, whereIndex).trim();
				wherePart = statement.trim().substring(whereIndex + 5).trim();
				String[] fromPartSplit = fromPart.split(",");

				tableMap.clear();

				for (int i = 0; i < fromPartSplit.length; i++) {
					tableMap.put(Utility.getNickName(fromPartSplit[i]), Utility.getRelationName(fromPartSplit[i]));
				}

				for (int j = 0; j < selectPartSplit.length; j++) {
					if (selectPartSplit[j].contains("*")) {
						continue;
					}
					if (!selectPartSplit[j].contains(".")) {
						print_error(11, "");
						return false;
					}
					key = Utility.getRelationName(selectPartSplit[j]);
					if (tableMap.containsKey(key)) {
						field = Utility.getNickName(selectPartSplit[j]);
						relationName = tableMap.get(key);
						long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);
						if (newRelationId != -1) {
							Relation newRelation = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId);
							Vector<Attribute> attributes = newRelation.getAttributes();
							boolean chk = true;
							for (int k = 0; k < attributes.size(); k++) {
								if (attributes.get(k).getName().equals(field)) {
									chk = false;
								}
							}
							if (chk) {
								print_error(1, field);
								return false;
							}
						} else {
							print_error(2, relationName);
							return false;
						}
					} else {
						print_error(3, key);
						return false;
					}
				}

				boolean whereClause = isCondition(wherePart);

				if (whereClause) {
					return true;
				} else {
					return false;
				}
			} else {
				fromPart = statement.substring(fromIndex + 4).trim();
				String[] fromPartSplit = fromPart.split(",");

				tableMap.clear();

				for (int i = 0; i < fromPartSplit.length; i++) {
					if (!fromPartSplit[i].contains("as")) {
						print_error(10, "");
						return false;
					}
					tableMap.put(Utility.getNickName(fromPartSplit[i]), Utility.getRelationName(fromPartSplit[i]));
				}

				for (int j = 0; j < selectPartSplit.length; j++) {
					if (selectPartSplit[j].contains("*")) {
						continue;
					}
					if (!selectPartSplit[j].contains(".")) {
						print_error(11, "");
						return false;
					}
					key = Utility.getRelationName(selectPartSplit[j]);
					if (tableMap.containsKey(key)) {
						field = Utility.getNickName(selectPartSplit[j]);
						relationName = tableMap.get(key);
						long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);
						if (newRelationId != -1) {
							Relation newRelation = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId);
							Vector<Attribute> attributes = newRelation.getAttributes();
							boolean chk = true;
							for (int k = 0; k < attributes.size(); k++) {

								if (attributes.get(k).getName().equals(field)) {
									chk = false;
								}
							}
							if (chk) {
								print_error(1, field);
								return false;
							}
						} else {
							print_error(2, relationName);
							return false;
						}
					} else {
						print_error(3, key);
						return false;
					}
				}
			}
		} else {
			print_error(4, "");
			return false;
		}
		return true;
	}

	static ConditionType getConditionType(String condition) {
		int lp, rp, i;
		if (condition == null) {
			return ConditionType.NULL;
		}
		condition = condition.trim().substring(1, condition.trim().length() - 1).trim();
		if (condition.charAt(0) == '(') {
			lp = 1;
			rp = 0;
			for (i = 1; i < condition.length() - 1; i++) {
				if (condition.charAt(i) == ')') {
					rp++;
				} else if (condition.charAt(i) == '(') {
					lp++;
				}
				if (lp == rp) {
					break;
				}
			}

			if (condition.substring(i + 1, i + 1 + condition.substring(i + 1).indexOf('(')).trim().toUpperCase().equals("AND")) {
				return ConditionType.AND;
			} else {
				return ConditionType.OR;
			}
		} else {
			return ConditionType.SIMPLE;
		}
	}

	static boolean isCondition(String s) {
		s = s.trim();
		String firstPart, lastPart;
		Vector<String> logicalOp, arithmeticOp;
		logicalOp = new Vector<String>();
		arithmeticOp = new Vector<String>();
		logicalOp.addElement("AND");
		logicalOp.addElement("OR");
		arithmeticOp.addElement("<=");
		arithmeticOp.addElement(">=");
		arithmeticOp.addElement("<");
		arithmeticOp.addElement(">");
		arithmeticOp.addElement("=");

		if (s.charAt(0) == '(' && s.charAt(s.length() - 1) == ')') {
			s = s.substring(1, s.length() - 1).trim();
			if (s.charAt(0) == '(' && s.charAt(s.length() - 1) == ')') {
				int lp = 1, rp = 0, i;
				for (i = 1; i < s.length() - 1; i++) {
					if (s.charAt(i) == ')') {
						rp++;
					} else if (s.charAt(i) == '(') {
						lp++;
					}
					if (lp == rp) {
						break;
					}
				}
				if (i < s.length() - 1 && logicalOp.contains(s.substring(i + 1, i + 1 + s.substring(i + 1).indexOf('(')).trim().toUpperCase())) {
					return isCondition(s.substring(0, i + 1)) && isCondition(s.substring(i + 1 + s.substring(i + 1).indexOf('(')));
				}
			} else if (!s.contains("(") && !s.contains(")")) {
				for (int j = 0; j < arithmeticOp.size(); j++) {
					if (s.toUpperCase().indexOf(arithmeticOp.get(j)) != -1) {
						firstPart = s.substring(0, s.toUpperCase().indexOf(arithmeticOp.get(j))).trim();
						lastPart = s.substring(s.toUpperCase().indexOf(arithmeticOp.get(j)) + arithmeticOp.get(j).length()).trim();
						/*
						 * first part check number , string , variable(s.id)
						 */
						if (Utility.isVariable(firstPart) && Utility.isVariable(lastPart)) {
							if (Utility.checkType(firstPart, lastPart)) {
								return true;
							} else {
								print_error(5, firstPart + " " + lastPart);
								return false;
							}
						} else if (Utility.isVariable(firstPart)) {
							String key = "", field = "", relationName = "";

							key = Utility.getRelationName(firstPart);
							field = Utility.getNickName(firstPart);
							relationName = tableMap.get(key);
							long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);

							if (newRelationId != -1) {
								Relation newRelation = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId);
								Attribute.Type firstPartType = newRelation.getAttributeType(field);

								if (Utility.isNum(lastPart)) {
									if (Utility.isSameType(firstPartType, lastPart)) {
										return true;
									} else {
										print_error(5, firstPart + " " + lastPart);
										return false;
									}
								} else if (Utility.isString(lastPart)) {
									if (Utility.isSameType(firstPartType, lastPart)) {
										return true;
									} else {
										print_error(5, firstPart + " " + lastPart);
										return false;
									}
								}
							}
						} else if (Utility.isVariable(lastPart)) {
							String key = "", field = "", relationName = "";

							key = Utility.getRelationName(lastPart);
							field = Utility.getNickName(lastPart);
							relationName = tableMap.get(key);
							long newRelationId = ObjectHolder.getObjectHolder().getRelationId(relationName);

							if (newRelationId != -1) {
								Relation newRelation = (Relation) ObjectHolder.getObjectHolder().getObject(newRelationId);
								Attribute.Type lastPartType = newRelation.getAttributeType(field);

								if (Utility.isNum(firstPart)) {
									if (Utility.isSameType(lastPartType, firstPart)) {
										return true;
									} else {
										print_error(5, firstPart + " " + lastPart);
										return false;
									}
								} else if (Utility.isString(firstPart)) {
									if (Utility.isSameType(lastPartType, firstPart)) {
										return true;
									} else {
										print_error(5, firstPart + " " + lastPart);
										return false;
									}
								}
							}
						}
						return true;
					}
				}
			}
		}
		print_error(6, " ");
		return false;
	}

	static OperatorType getOperatorType(String statement) {
		if (statement.contains("<=")) {
			return OperatorType.LESSEQUAL;
		} else if (statement.contains(">=")) {
			return OperatorType.GREATEQUAL;
		} else if (statement.contains("<")) {
			return OperatorType.LESS;
		} else if (statement.contains(">")) {
			return OperatorType.GREATER;
		} else {
			return OperatorType.EQUAL;
		}
	}

	static String getLeftOperand(String statement, OperatorType opType) {
		String operator = OperatorType.toString(opType);
		int index = statement.indexOf(operator);
		return statement.substring(0, index).trim();
	}

	static String getRightOperand(String statement, OperatorType opType) {
		String operator = OperatorType.toString(opType);
		int index = statement.indexOf(operator);
		return statement.substring(index + operator.length()).trim();
	}

	public static void print_error(int i, String s) {
		String[] t = s.split(" ");
		switch (i) {
		case 1:
			System.out.println("Error: Attribute " + s + " is not a valid attribute. \n");
			break;
		case 2:
			System.out.println("Error: "+s + " is not a valid Relation Name. \n");
			break;
		case 3:
			System.out.println("Error: "+s + " is not a valid Relation Instance. \n");
			break;
		case 4:
			System.out.println("Error:  Not a valid select Syntax. \n");
			break;
		case 5:
			System.out.println(t[0] + " and " + t[1] + " are not of Same Type. \n");
			break;
		case 6:
			System.out.println("Error:  ( or ) brackets expected. \n");
			break;
		case 7:
			System.out.println("Error:  Not a valid update Syntax. \n");
			break;
		case 8:
			System.out.println("Error:  Column " + s + "does not exits. \n");
			break;
		case 9:
			System.out.println("Error:  Keyword table does not exits. \n");
			break;
		case 10:
			System.out.println("Error:  \'as\' Expected \n");
			break;
		case 11:
			System.out.println("Error:   \'.\' or \'*\' Expected. \n");
			break;
		case 12:
			System.out.println("Error:  Not a valid create Syntax. \n");
			break;
		default:
			System.out.println("Error: undefined, see error message ");
			break;
		}
	}

	static Vector<String> getJoinTableList(String statement, String JoinName) {
		Vector<String> result;
		statement = statement.replace("join", "").trim();
		String[] tableList = statement.split(JoinName);
		result = new Vector<String>(tableList.length);
		for (int i = 0; i < tableList.length; i++) {
			result.add(i, tableList[i].trim());
		}
		return result;
	}
}
