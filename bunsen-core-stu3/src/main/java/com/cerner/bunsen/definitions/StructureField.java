package com.cerner.bunsen.definitions;

public class StructureField<T> {

  private final String propertyName;

  private final String fieldName;

  private final String extensionUrl;

  private final boolean isChoice;

  private final T visitorResult;

  public StructureField(String propertyName,
      String fieldName,
      String extensionUrl,
      boolean isChoice,
      T visitorResult) {

    this.propertyName = propertyName;
    this.fieldName = fieldName;
    this.extensionUrl = extensionUrl;
    this.isChoice = isChoice;
    this.visitorResult = visitorResult;
  }

  public String propertyName() {

    return propertyName;
  }

  public String fieldName() {

    return fieldName;
  }

  public String extensionUrl() {

    return extensionUrl;
  }

  public boolean isChoice() {

    return isChoice;
  }

  public T result() {
    return visitorResult;
  }

  public static <T>  StructureField<T> property(String propertyName, T visitorResult) {

    return new StructureField<T>(propertyName, propertyName, null, false, visitorResult);
  }

  public static <T>  StructureField<T> extension(String fieldName, String extensionUrl, T visitorResult) {

    return new StructureField<T>(null, fieldName, extensionUrl, false, visitorResult);
  }

}
