package com.cerner.bunsen.definitions;

import java.util.List;
import java.util.Map;

/**
 * Visitor for each field in a FHIR StructureDefinition
 *
 * @param <T>
 */
public interface DefinitionVisitor<T> {

  public T visitPrimitive(String elementName,
      String primitiveType);

  public T visitComposite(String elementName,
      String elementType,
      List<StructureField<T>> children);

  public T visitReference(String elementName,
      List<String> referenceTypes,
      List<StructureField<T>> children);

  public T visitParentExtension(String elementName,
      String extensionUrl,
      List<StructureField<T>> children);

  public T visitLeafExtension(String elementName,
      String extensionUri,
      T element);

  public T visitMultiValued(String elementName,
      T arrayElement);

  public T visitChoice(String elementName,
      Map<String,T> fhirToChoiceTypes);
}
