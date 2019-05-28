package com.ciessa.museum.dbconvert;

public class FieldDescription {

	private String file;
	private String library;
	private String fileType;
	private String field;
	private int lenght;
	private int digits;
	private int decimals;
	private String text;
	private String fieldType;
	private String originalField;
	
	public FieldDescription () {
		super();
	}
	
	public String safeString(String input) {
		String ret = input.replaceAll("ACAPA\\$", "ACAPAS1");
		ret = ret.replaceAll("\\$", "S");
		ret = ret.replaceAll("Ñ", "N");
		ret = ret.replaceAll("ñ", "n");
		return ret;
	}
	
	public FieldDescription(String file, String library, String fileType, String field, int lenght, int digits,
			int decimals, String text, String fieldType) {
		super();
		this.file = safeString(file);
		this.library = safeString(library);
		this.fileType = safeString(fileType);
		this.field = safeString(field);
		this.originalField = field;
		this.lenght = lenght;
		this.digits = digits;
		this.decimals = decimals;
		this.text = text;
		this.fieldType = safeString(fieldType);
	}

	/**
	 * @return the file
	 */
	public String getFile() {
		return file;
	}

	/**
	 * @param file the file to set
	 */
	public void setFile(String file) {
		this.file = file;
	}

	/**
	 * @return the library
	 */
	public String getLibrary() {
		return library;
	}

	/**
	 * @param library the library to set
	 */
	public void setLibrary(String library) {
		this.library = library;
	}

	/**
	 * @return the fileType
	 */
	public String getFileType() {
		return fileType;
	}

	/**
	 * @param fileType the fileType to set
	 */
	public void setFileType(String fileType) {
		this.fileType = fileType;
	}

	/**
	 * @return the field
	 */
	public String getField() {
		return field;
	}

	/**
	 * @param field the field to set
	 */
	public void setField(String field) {
		this.field = field;
	}

	/**
	 * @return the lenght
	 */
	public int getLenght() {
		return lenght;
	}

	/**
	 * @param lenght the lenght to set
	 */
	public void setLenght(int lenght) {
		this.lenght = lenght;
	}

	/**
	 * @return the digits
	 */
	public int getDigits() {
		return digits;
	}

	/**
	 * @param digits the digits to set
	 */
	public void setDigits(int digits) {
		this.digits = digits;
	}

	/**
	 * @return the decimals
	 */
	public int getDecimals() {
		return decimals;
	}

	/**
	 * @param decimals the decimals to set
	 */
	public void setDecimals(int decimals) {
		this.decimals = decimals;
	}

	/**
	 * @return the text
	 */
	public String getText() {
		return text;
	}

	/**
	 * @param text the text to set
	 */
	public void setText(String text) {
		this.text = text;
	}

	/**
	 * @return the fieldType
	 */
	public String getFieldType() {
		return fieldType;
	}

	/**
	 * @param fieldType the fieldType to set
	 */
	public void setFieldType(String fieldType) {
		this.fieldType = fieldType;
	}

	/**
	 * @return the originalField
	 */
	public String getOriginalField() {
		return originalField;
	}

	/**
	 * @param originalField the originalField to set
	 */
	public void setOriginalField(String originalField) {
		this.originalField = originalField;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "FieldDescription [file=" + file + ", library=" + library + ", fileType=" + fileType + ", field=" + field
				+ ", lenght=" + lenght + ", digits=" + digits + ", decimals=" + decimals + ", text=" + text
				+ ", fieldType=" + fieldType + "]";
	}
	
}
