/**
 * 
 */
package com.etao.hbase.coprocessor.compare.rule;

import java.util.regex.Pattern;

/**
 * @author dihong.wq
 * 
 */
public class CompareRule {
	private static final String WILDCHAR_REGEX = "\\*";
	private static final String WILDCHAR_REPLACE = "[\\\\p{ASCII}]*";

	private String column;
	private Trigger trigger;
	private Action action;

	private String family;
	private String qualifier;

	private Pattern pattern = null;

	@Override
	public String toString() {
		return "[column=" + column + ", trigger=" + trigger + ", action="
				+ action + "]";
	}

	public boolean isExactMatch() {
		return pattern == null;
	}

	public boolean matchesFamily(String family) {
		return this.family.equals(family);
	}

	public boolean matchesQualifier(String qualifier) {
		return pattern == null ? this.qualifier.equals(qualifier) : pattern
				.matcher(qualifier).matches();
	}

	public boolean matches(String family, String qualifier) {
		return this.family.equals(family)
				&& (pattern == null ? this.qualifier.equals(qualifier)
						: pattern.matcher(qualifier).matches());
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
		int deliIdx = column.indexOf(':');
		family = this.column.substring(0, deliIdx);
		qualifier = this.column.substring(deliIdx + 1);

		int regIdx = qualifier.indexOf('*');
		if (regIdx >= 0) {
			pattern = Pattern.compile(qualifier.replaceAll(WILDCHAR_REGEX,
					WILDCHAR_REPLACE));
		}
	}

	public String getFamily() {
		return family;
	}

	public Trigger getTrigger() {
		return trigger;
	}

	public void setTrigger(Trigger trigger) {
		this.trigger = trigger;
	}

	public Action getAction() {
		return action;
	}

	public void setAction(Action action) {
		this.action = action;
	}
}
