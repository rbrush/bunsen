package com.cerner.bunsen.definitions.stu3;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.support.IContextValidationSupport;
import com.cerner.bunsen.definitions.DefinitionVisitor;
import com.cerner.bunsen.definitions.FhirConversionSupport;
import com.cerner.bunsen.definitions.StructureDefinitions;
import com.cerner.bunsen.definitions.StructureField;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;

import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hl7.fhir.dstu3.model.ElementDefinition;
import org.hl7.fhir.dstu3.model.ElementDefinition.TypeRefComponent;
import org.hl7.fhir.dstu3.model.StructureDefinition;


public class Stu3StructureDefinitions extends StructureDefinitions<StructureDefinition> {

  private static final FhirConversionSupport CONVERSION_SUPPORT = new Stu3FhirConversionSupport();

  /**
   * Returns the immediate children of the given element from the list of all defined
   * elements in the structure definition.
   *
   * @param parent the element to get the children for
   * @param definitions the full list of element definitions
   * @return the list of elements that are children of the given element
   */
  private List<ElementDefinition> getChildren(ElementDefinition parent,
      List<ElementDefinition> definitions) {

    String startsWith = parent.getId() + ".";

    // Get nodes
    return definitions.stream().filter(definition ->
        definition.getId().startsWith(startsWith)
            && definition.getId().indexOf('.', startsWith.length()) < 0)
        .collect(Collectors.toList());
  }

  public Stu3StructureDefinitions(FhirContext context) {

    super(context);
  }

  private String elementName(ElementDefinition element) {

    String suffix = element.getPath().substring(element.getPath().lastIndexOf(".") + 1);

    // Remove the [x] suffix used by choise types, if applicable.
    return suffix.endsWith("[x]")
        ? suffix.substring(0, suffix.length() - 3)
        : suffix;
  }

  /**
   * Returns the StructureDefinition for the given element if it is an
   * externally defined datatype. Returns null otherwise.
   */
  private StructureDefinition getDefinition(ElementDefinition element) {

    // Elements that don't specify a type or are backbone elements defined
    // within the parent structure do not have a separate structure definition.
    return element.getTypeFirstRep() == null
        || element.getTypeFirstRep().getCode() == null
        || element.getTypeFirstRep().getCode().equals("BackboneElement")
        ? null
        : (StructureDefinition) validationSupport.fetchStructureDefinition(
            context, element.getTypeFirstRep().getCode());
  }

  private <T> List<StructureField<T>> extensionElementToFields(DefinitionVisitor<T> visitor,
      ElementDefinition element,
      List<ElementDefinition> definitions,
      Deque<StructureDefinition> stack) {

    // FIXME: extension is a type rather than an external structure....
    StructureDefinition definition = (StructureDefinition) validationSupport.fetchStructureDefinition(
        context, element.getTypeFirstRep().getProfile());

    if (definition != null) {

      if (shouldTerminateRecursive(definition, stack)) {

        return Collections.emptyList();

      } else {

        List<ElementDefinition> extensionDefinitions = definition.getSnapshot().getElement();

        ElementDefinition extensionRoot = extensionDefinitions.get(0);

        return visitExtensionDefinition(visitor, element.getSliceName(),
            stack,
            definition.getUrl(),
            extensionDefinitions,
            extensionRoot);
      }

    } else {

      if (element.getSliceName() == null) {
        return Collections.emptyList();
      }

      return visitExtensionDefinition(visitor, element.getSliceName(),
          stack,
          element.getTypeFirstRep().getProfile(),
          definitions,
          element);

      // return visitor.visitParentExtension(parent, element.getSliceName(), element, childElements);
    }
  }

  private <T> List<StructureField<T>> visitExtensionDefinition(DefinitionVisitor<T> visitor,
      String sliceName,
      Deque<StructureDefinition> stack,
      String url,
      List<ElementDefinition> extensionDefinitions,
      ElementDefinition extensionRoot) {

    List<ElementDefinition> children = getChildren(extensionRoot, extensionDefinitions);

    // Extensions may contain either additional extensions or a value field, but not both.

    List<ElementDefinition> childExtensions = children.stream()
        .filter( e -> e.getSliceName() != null)
        .collect(Collectors.toList());

    if (!childExtensions.isEmpty()) {

      List<StructureField<T>> childFields = new ArrayList<>();

      for(ElementDefinition childExtension: childExtensions) {
        List<StructureField<T>> childField = extensionElementToFields(visitor,
            childExtension, extensionDefinitions, stack);

        childFields.addAll(childField);
      }

      // The extension has child extensions, so recursively evaluate them.
      // Map<String,T> childElements = transformChildren(parent, extensionRoot, childExtensions, stack);

      T result = visitor.visitParentExtension(sliceName,
          url,
          childFields);

      if (result == null) {
        return Collections.emptyList();
      } else {
        return Collections.singletonList(
            StructureField.extension(sliceName,
                url,
                result));
      }

    } else {

      // The extension has no children, so produce its value.

      Optional<ElementDefinition> valueElement = children.stream()
          .filter(e -> e.getPath().contains("value"))
          .findFirst();

      // FIXME: get the extension URL.
      Optional<ElementDefinition> urlElement = children.stream()
          .filter(e -> e.getPath().endsWith("url"))
          .findFirst();

      String extensionUrl = urlElement.get().getFixed().primitiveValue();


      List<StructureField<T>> childField = elementToFields(visitor, valueElement.get(), extensionDefinitions, stack);

      T result = visitor.visitLeafExtension(sliceName,
          extensionUrl,
          childField.iterator().next().result());

      return Collections.singletonList(
          StructureField.extension(sliceName,
              extensionUrl,
              result));

    }
  }

  private <T> List<StructureField<T>> visitComposite(DefinitionVisitor<T> visitor,
      String elementName,
      String elementType,
      List<StructureField<T>> childElements) {

    return singleField(elementName,
        visitor.visitComposite(elementName, elementType, childElements));
  }

  private <T> List<StructureField<T>> singleField(String elementName, T result) {

    return Collections.singletonList(StructureField.property(elementName, result));
  }

  /**
   * Returns the fields for the given element. The returned stream can be empty
   * (e.g., for elements with max of zero), or have multiple values (for elements
   * that generate fields with additional data in siblings.)
   *
   * @param element
   * @param definitions
   * @return
   */
  private <T> List<StructureField<T>> elementToFields(DefinitionVisitor<T> visitor,
      ElementDefinition element,
      List<ElementDefinition> definitions,
      Deque<StructureDefinition> stack) {

    String elementName = elementName(element);

    // Fields with max of zero are omitted.
    if (element.getMax().equals("0")) {

      return Collections.emptyList();

    } else if("Extension".equals(element.getTypeFirstRep().getCode())) {

      return extensionElementToFields(visitor, element, definitions, stack);

    } else if (element.getType().size() == 1
        && PRIMITIVE_TYPES.contains(element.getTypeFirstRep().getCode())) {


      return singleField(elementName,
          visitor.visitPrimitive(elementName, element.getTypeFirstRep().getCode()));

    } else if (element.getPath().endsWith("[x]")) {

      // Use a linked hash map to preserve the order of the fields
      // for iteration.
      Map<String,T> choiceTypes = new LinkedHashMap<>();

      for (TypeRefComponent typeRef: element.getType()) {

        if (PRIMITIVE_TYPES.contains(typeRef.getCode().toLowerCase())) {

          T child = visitor.visitPrimitive(elementName, typeRef.getCode().toLowerCase());
          choiceTypes.put(typeRef.getCode(), child);

        } else {

          StructureDefinition structureDefinition =
              (StructureDefinition) validationSupport.
                  fetchStructureDefinition(context, typeRef.getCode());

          T child = transform(visitor, element, structureDefinition, new ArrayDeque<>());

          choiceTypes.put(typeRef.getCode(), child);

        }
      }

      StructureField<T> field = new StructureField<T>(elementName,
          elementName,
          null,
          true,
          visitor.visitChoice(elementName, choiceTypes));

      return Collections.singletonList(field);

    } else if (!element.getMax().equals("1")) {

      if (getDefinition(element) != null) {

        // Handle defined data types.
        StructureDefinition definition = getDefinition(element);

        if (shouldTerminateRecursive(definition, stack)) {

          return Collections.emptyList();

        } else {

          T type = transform(visitor, element, definition, stack);

          return singleField(elementName,
              visitor.visitMultiValued(elementName, type));
        }

      } else {

        List<StructureField<T>> childElements = transformChildren(visitor,
            element, definitions, stack);

        List<StructureField<T>> composite = visitComposite(visitor, elementName, null, childElements);

        // Array types should produce only a single element.
        if (composite.size() != 1) {
          throw new IllegalStateException("Array type in "
              + element.getPath()
              + " must map to a single structure.");
        }

        // Wrap the item in the corresponding multi-valued type.
        return singleField(elementName,
            visitor.visitMultiValued(elementName, composite.get(0).result()));

      }

    } else if (getDefinition(element) != null) {

      // Handle defined data types.
      StructureDefinition definition = getDefinition(element);

      if (shouldTerminateRecursive(definition, stack)) {

        return Collections.emptyList();

      } else {
        T type = transform(visitor, element, definition, stack);

        return singleField(elementName(element), type);
      }

    } else {

      // Handle composite type
      List<StructureField<T>> childElements = transformChildren(visitor,
          element, definitions, stack);

      // The child elements have been created, so
      return visitComposite(visitor, elementName, null, childElements);

    }
  }

  /**
   * Transform methods of child elements.
   */
  private <T> List<StructureField<T>> transformChildren(DefinitionVisitor<T> visitor,
      ElementDefinition element,
      List<ElementDefinition> definitions,
      Deque<StructureDefinition> stack) {

    // Handle composite type
    List<StructureField<T>> childElements = new ArrayList<>();

    for (ElementDefinition child: getChildren(element, definitions)) {

      List<StructureField<T>>childFields = elementToFields(visitor,  child, definitions, stack);

      childElements.addAll(childFields);
    }

    return childElements;
  }

  private boolean shouldTerminateRecursive(StructureDefinition definition,
      Deque<StructureDefinition> stack) {

    // TODO: make recursive depth configurable?
    return stack.stream().filter(def -> def.getUrl().equals(definition.getUrl())).count() > 0;
  }

  @Override
  public <T> T transform(DefinitionVisitor<T> visitor,  String resourceTypeUrl) {

    StructureDefinition definition = (StructureDefinition) context.getValidationSupport()
        .fetchStructureDefinition(context, resourceTypeUrl);

    return transform(visitor, definition);
  }

  @Override
  public FhirConversionSupport conversionSupport() {

    return CONVERSION_SUPPORT;
  }

  /**
   * Returns the Spark struct type used to encode the given FHIR composite.
   *
   * @return The schema as a Spark StructType
   */
  public <T> T transform(DefinitionVisitor<T> visitor,  StructureDefinition definition) {

    return transform(visitor, null, definition, new ArrayDeque<>());
  }

  /**
   * Returns an ordered map of transformations for each field in the definition.
   */
  public <T> List<StructureField<T>> transformChildren(DefinitionVisitor<T> visitor,
      StructureDefinition definition) {

    List<ElementDefinition> definitions = definition.getSnapshot().getElement();

    ElementDefinition root = definitions.get(0);
    
    return transformChildren(visitor, root, definitions, new ArrayDeque<>());
  }

  /**
   * Returns the Spark struct type used to encode the given FHIR composite.
   *
   * @return The schema as a Spark StructType
   */
  private <T> T transform(DefinitionVisitor<T> visitor,
      ElementDefinition element,
      StructureDefinition definition,
      Deque<StructureDefinition> stack) {

    List<ElementDefinition> definitions = definition.getSnapshot().getElement();

    ElementDefinition root = definitions.get(0);

    stack.push(definition);

    List<StructureField<T>> childElements = transformChildren(visitor, root, definitions, stack);

    stack.pop();

    if ("Reference".equals(definition.getType())) {

      // TODO: if this is in an option there may be other non-reference types here?
      String rootName = elementName(root);

      List<String> referenceTypes = element.getType()
          .stream()
          .filter(type -> "Reference".equals(type.getCode()))
          .map(type -> {

            IContextValidationSupport validation = context.getValidationSupport();

            StructureDefinition targetDefinition = (StructureDefinition)
                validation.fetchStructureDefinition(context, type.getTargetProfile());

            return targetDefinition.getType();
          })
          .collect(Collectors.toList());

      return visitor.visitReference(rootName, referenceTypes, childElements);

    } else {
      String rootName = elementName(root);

      // The child elements have been created, so
      return visitComposite(visitor, rootName, definition.getType(), childElements).get(0).result();
    }

  }

}
