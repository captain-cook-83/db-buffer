package com.karma.dbbuffer.client.jctree;

import com.alibaba.fastjson.JSON;
import com.google.auto.service.AutoService;
import com.karma.dbbuffer.Constants;
import com.karma.dbbuffer.client.entity.PBigEntity;
import com.karma.dbbuffer.client.entity.PSmallEntity;
import com.karma.dbbuffer.schema.FieldDefinition;
import com.karma.dbbuffer.schema.FieldType;
import com.karma.dbbuffer.schema.VersionableSchema;
import com.sun.source.util.TreePath;
import com.sun.tools.javac.code.Flags;
import com.sun.tools.javac.code.Symbol;
import com.sun.tools.javac.code.Type;
import com.sun.tools.javac.code.TypeTag;
import com.sun.tools.javac.processing.JavacProcessingEnvironment;
import com.sun.source.tree.Tree;
import com.sun.tools.javac.api.JavacTrees;
import com.sun.tools.javac.model.JavacElements;
import com.sun.tools.javac.tree.JCTree;
import com.sun.tools.javac.tree.TreeMaker;
import com.sun.tools.javac.tree.TreeTranslator;
import com.sun.tools.javac.util.*;
import com.sun.tools.javac.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;
import java.io.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * https://www.jianshu.com/p/4bd5dc13f35a
 * https://www.shuzhiduo.com/A/1O5EqqOr57/
 */

@SupportedAnnotationTypes("com.karma.dbbuffer.client.jctree.PRecord")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
@AutoService(Processor.class)
public class PRecordProcessor extends AbstractProcessor {

    private static final String GET = "get";

    private static final String SET = "set";

    private static final String THIS = "this";

    private static final String GETTER_PARAM_INDEX = "index";

    private static final String VARIABLE_ID = "id";

    private static final String VARIABLE_INSERT_MASK = "_$insertMask";

    private static final String VARIABLE_SET_MASK = "_$setMask";

    private static final String VARIABLE_UNSET_MASK = "_$unsetMask";

    private static final String VARIABLE_SCHEMA_ID = "_$schemaId";

    private static final String VARIABLE_SCHEMA_NAME = "_$schemaName";

    private static final String VARIABLE_SCHEMA_VERSION = "_$schemaVersion";

    private static final String VARIABLE_SCHEMA_SORTING = "_$schemaSortingOrder";

    private static final String VARIABLE_FIELD_DEFINITIONS = "_$fieldDefinitions";

    private static final String METHOD_CLEAR_ALL_MASK = "_$clearAllMask";

    private static final String METHOD_GET_PRIMITIVE_BOOLEAN = "get_$PrimitiveBoolean";

    private static final String METHOD_GET_PRIMITIVE_BYTE = "get_$PrimitiveByte";

    private static final String METHOD_GET_PRIMITIVE_SHORT = "get_$PrimitiveShort";

    private static final String METHOD_GET_PRIMITIVE_INT = "get_$PrimitiveInt";

    private static final String METHOD_GET_PRIMITIVE_LONG = "get_$PrimitiveLong";

    private static final String METHOD_GET_INTEGER = "get_$Integer";

    private static final String METHOD_GET_LONG = "get_$Long";

    private static final String METHOD_GET_STRING = "get_$String";

    private static final Map<String, Object> METHOD_TYPE_MAP = new HashMap<>();

    static {
        METHOD_TYPE_MAP.put(METHOD_GET_PRIMITIVE_BOOLEAN, TypeTag.BOOLEAN);
        METHOD_TYPE_MAP.put(METHOD_GET_PRIMITIVE_BYTE, TypeTag.BYTE);
        METHOD_TYPE_MAP.put(METHOD_GET_PRIMITIVE_SHORT, TypeTag.SHORT);
        METHOD_TYPE_MAP.put(METHOD_GET_PRIMITIVE_INT, TypeTag.INT);
        METHOD_TYPE_MAP.put(METHOD_GET_PRIMITIVE_LONG, TypeTag.LONG);
        METHOD_TYPE_MAP.put(METHOD_GET_INTEGER, "Integer");
        METHOD_TYPE_MAP.put(METHOD_GET_LONG, "Long");
        METHOD_TYPE_MAP.put(METHOD_GET_STRING, "String");
    }

    private Context context;

    private JavacTrees trees;

    private TreeMaker treeMaker;

    private Names names;

    private Messager messager;

    private JCTree.JCLiteral oneValue;

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
//        processingEnv = jbUnwrap(ProcessingEnvironment.class, processingEnv);
        trees = JavacTrees.instance(processingEnv);
        context = ((JavacProcessingEnvironment) processingEnv).getContext();
        treeMaker = TreeMaker.instance(context);
        names = Names.instance(context);
        messager = processingEnv.getMessager();
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        final Set<? extends Element> recordAnnotations = roundEnv.getElementsAnnotatedWith(PRecord.class);
        recordAnnotations.stream().forEach(element -> {
            JCTree tree = trees.getTree(element);
            tree.accept(new TreeTranslator() {

                @Override
                public void visitClassDef(JCTree.JCClassDecl jcClassDecl) {
                    final Map<Name, JCTree.JCVariableDecl> variableDeclMap =
                            jcClassDecl.defs.stream().filter(tree -> tree.getKind().equals(Tree.Kind.VARIABLE))
                                    .map(tree -> (JCTree.JCVariableDecl) tree)
                                    .collect(Collectors.toMap(JCTree.JCVariableDecl::getName, Function.identity()));
                    final Map<Name, Integer> indexMap = variableDeclMap.entrySet().stream().map(entry -> createNameIndexPair(entry)).filter(
                            e -> e != null).collect(Collectors.toMap(NameIndexPair::getName, NameIndexPair::getIndex));
                    JCTree.JCVariableDecl idVariable = variableDeclMap.get(names.fromString(VARIABLE_ID));
                    if (idVariable == null) {
                        messager.printMessage(Diagnostic.Kind.ERROR, String.format("missing %s field for %s", VARIABLE_ID, jcClassDecl.name.toString()));
                    } else {
                        JCTree.JCAnnotation idFieldAnnotation = getAnnotation(idVariable.getModifiers(), PField.class);
                        JCTree.JCExpression idFieldIndex = getAnnotationAttribute(idFieldAnnotation, "value");
                        if (!Objects.equals(idFieldIndex.toString(), "0")) {
                            messager.printMessage(Diagnostic.Kind.ERROR, String.format("@PField.value of id field for %s must be fixed 0", jcClassDecl.name.toString()));
                        }
                    }

                    Map<Integer, Name> nameMap = new HashMap<>(indexMap.size());
                    for (Map.Entry<Name, Integer> entry : indexMap.entrySet()) {
                        Name existName = nameMap.get(entry.getValue());
                        if (existName != null) {
                            messager.printMessage(Diagnostic.Kind.ERROR, String.format("duplicated index from %s: %s and %s", jcClassDecl.name.toString(), entry.getKey(), existName.toString()));
                        }
                        nameMap.put(entry.getValue(), entry.getKey());
                    }

                    final Map<Name, JCTree.JCMethodDecl> methodDeclMap =
                            jcClassDecl.defs.stream().filter(tree -> tree.getKind().equals(Tree.Kind.METHOD))
                                    .map(tree -> (JCTree.JCMethodDecl) tree)
                                    .collect(Collectors.toMap(JCTree.JCMethodDecl::getName, Function.identity()));

                    boolean isSmallEntity = indexMap.values().stream().max(Integer::compare).get() < Constants.MAX_SIZE_OF_SMALL_ENTITY;
                    TypeTag typeTag = isSmallEntity ? TypeTag.INT : TypeTag.LONG;
                    Class<?> interfaceClass = isSmallEntity ? PSmallEntity.class : PBigEntity.class;
                    oneValue = isSmallEntity ? treeMaker.Literal(1) : treeMaker.Literal(1L);

                    TreePath treePath = trees.getPath(element);
                    JCTree.JCCompilationUnit compilationUnit = (JCTree.JCCompilationUnit) treePath.getCompilationUnit();

                    JCTree packageInfo = compilationUnit.defs.head;
                    compilationUnit.defs = compilationUnit.defs.prepend(createImport(interfaceClass));                      // import com.karma.dbbuffer.client.entity.PSmallEntity/PBigEntity;
                    compilationUnit.defs = compilationUnit.defs.prepend(createImport(FieldDefinition.class));               // import com.karma.dbbuffer.schema.FieldDefinition;
                    compilationUnit.defs = compilationUnit.defs.prepend(createImport(FieldType.class));                     // import com.karma.dbbuffer.schema.FieldType;
                    // must prepend the packageInfo, the head of defs is always the fixed package info !!!
                    compilationUnit.defs = compilationUnit.defs.prepend(packageInfo);

                    Symbol.ClassSymbol interfaceClassSymbol =
                            new Symbol.ClassSymbol(0, names.fromString(interfaceClass.getSimpleName()), idVariable.type, null);
                    JCTree.JCIdent interfaceIdent = treeMaker.Ident(interfaceClassSymbol);
                    jcClassDecl.implementing = jcClassDecl.implementing.prepend(interfaceIdent);            // implements PSmallEntity<T>/PBigEntity<T>

                    JCTree.JCVariableDecl setMaskVar = numberVariable(VARIABLE_SET_MASK, typeTag, treeMaker.Literal(0), false);                            // private int _$setMask = 0;
                    JCTree.JCVariableDecl unsetMaskVar = numberVariable(VARIABLE_UNSET_MASK, typeTag, treeMaker.Literal(0), false);                        // private int _$unsetMask = 0;
                    jcClassDecl.defs = jcClassDecl.defs.prepend(setMaskVar).prepend(unsetMaskVar).
                            append(generateGetterMethod(setMaskVar, typeTag)).                                                  // public int get_$setMask() { return this._$setMask; }
                            append(generateGetterMethod(unsetMaskVar, typeTag));                                                // public int get_$unsetMask() { return this._$unsetMask; }

                    java.util.List<Map.Entry<Name, Integer>> sortedNameIndexes =
                            indexMap.entrySet().stream().sorted(Comparator.comparingInt(Map.Entry::getValue)).collect(Collectors.toList());

                    JCTree.JCVariableDecl fieldDefinitionVar = createFieldDefinitionsVariable(jcClassDecl, variableDeclMap, sortedNameIndexes);
                    JCTree.JCVariableDecl schemaNameVar = createSchemaVariable(jcClassDecl, VARIABLE_SCHEMA_NAME, "name", treeMaker.Ident(names.fromString(String.class.getSimpleName())));
                    JCTree.JCVariableDecl schemaIdVar = createSchemaVariable(jcClassDecl, VARIABLE_SCHEMA_ID, "id", treeMaker.TypeIdent(TypeTag.BYTE));
                    JCTree.JCVariableDecl schemaVersionVar = createSchemaVariable(jcClassDecl, VARIABLE_SCHEMA_VERSION, "version", treeMaker.TypeIdent(TypeTag.BYTE));
                    JCTree.JCVariableDecl schemaSoringOrderVar = createSchemaVariable(jcClassDecl, VARIABLE_SCHEMA_SORTING, "sortingOrder", treeMaker.TypeIdent(TypeTag.BOOLEAN));
                    jcClassDecl.defs = jcClassDecl.defs.prepend(fieldDefinitionVar);                        // private static final FieldDefinition[] _$fieldDefinitions = new FieldDefinition[]{ new FieldDefinition(...), ... };
                    jcClassDecl.defs = jcClassDecl.defs.prepend(schemaNameVar);                             // private static final String _$schemaName = "name";
                    jcClassDecl.defs = jcClassDecl.defs.prepend(schemaIdVar);                               // private static final byte _$schemaId = 0;
                    jcClassDecl.defs = jcClassDecl.defs.prepend(schemaVersionVar);                          // private static final byte _$schemaVersion = 0;
                    jcClassDecl.defs = jcClassDecl.defs.prepend(schemaSoringOrderVar);                      // private static final boolean _$schemaSortingOrder = false;
                    jcClassDecl.defs = jcClassDecl.defs.append(generateGetterStaticMethod(jcClassDecl, schemaIdVar));
                    jcClassDecl.defs = jcClassDecl.defs.append(generateGetterStaticMethod(jcClassDecl, schemaNameVar));
                    jcClassDecl.defs = jcClassDecl.defs.append(generateGetterStaticMethod(jcClassDecl, schemaVersionVar));
                    jcClassDecl.defs = jcClassDecl.defs.append(generateGetterStaticMethod(jcClassDecl, schemaSoringOrderVar));
                    jcClassDecl.defs = jcClassDecl.defs.append(generateGetterStaticMethod(jcClassDecl, fieldDefinitionVar));

                    long longInsertMask = 0;
                    for (Map.Entry<Name, Integer> entry : sortedNameIndexes) {
                        JCTree.JCVariableDecl var = variableDeclMap.get(entry.getKey());
                        FieldType fieldType = FieldType.valueOfDef(var.vartype.toString());
                        if (fieldType.isPrimitiveType() || var.init != null) {
                            longInsertMask |= 1L << entry.getValue();
                        }
                    }

                    JCTree.JCVariableDecl insertMaskVar =
                            numberVariable(VARIABLE_INSERT_MASK, typeTag, isSmallEntity ? treeMaker.Literal((int) longInsertMask) : treeMaker.Literal(longInsertMask), true);
                    jcClassDecl.defs = jcClassDecl.defs.prepend(insertMaskVar);                                             // private static final int _$insertMask = xxx;
                    jcClassDecl.defs = jcClassDecl.defs.append(generateGetterStaticMethod(jcClassDecl, insertMaskVar));

                    // public void setXXX(XXX xxx) { this.xxx = xxx; ... }
                    indexMap.forEach((name, index) -> {
                        JCTree.JCVariableDecl var = variableDeclMap.get(name);
                        JCTree.JCMethodDecl methodDecl = methodDeclMap.get(methodName(name, SET));
                        boolean checkNull = !(var.vartype instanceof JCTree.JCPrimitiveTypeTree);
                        if (methodDecl != null) {
                            appendMaskAssignmentToMethod(var, methodDecl, index, checkNull);
                        } else {
                            try {
                                jcClassDecl.defs = jcClassDecl.defs.prepend(generateSetterMethod(var, index, checkNull));
                            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException exception) {
                                messager.printMessage(Diagnostic.Kind.ERROR, exception.getMessage());
                            }
                        }
                    });

                    try {
                        jcClassDecl.defs = jcClassDecl.defs.prepend(generateClearMethod());
                    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException exception) {
                        messager.printMessage(Diagnostic.Kind.ERROR, exception.getMessage());
                    }

                    List<JCTree> methodDecls = generateRouteGetterMethods(variableDeclMap, indexMap);
                    jcClassDecl.defs = jcClassDecl.defs.appendList(methodDecls);

                    super.visitClassDef(jcClassDecl);
                }
            });
        });

        return false;
    }

    private JCTree.JCImport createImport(Class<?> clazz) {
        String fullName = clazz.getCanonicalName();
        int sepIndex = fullName.lastIndexOf('.');
        String name = fullName.substring(sepIndex + 1);
        return treeMaker.Import(treeMaker.Select(
                treeMaker.Ident(names.fromString(fullName.substring(0, sepIndex))), names.fromString(name)), false);
    }

    private JCTree.JCVariableDecl createSchemaVariable(JCTree.JCClassDecl classDecl, String variableName, String attributeName, JCTree.JCExpression type) {
        final JCTree.JCModifiers modifiers = treeMaker.Modifiers(Flags.PRIVATE | Flags.STATIC | Flags.FINAL);
        final Name varName = names.fromString(variableName);
        JCTree.JCAnnotation annotation = getAnnotation(classDecl.getModifiers(), PRecord.class);
        JCTree.JCExpression versionExp = getAnnotationAttribute(annotation, attributeName);
        if (versionExp == null) {
            versionExp = treeMaker.Literal(TypeTag.BOOLEAN, 0);
        }
        return treeMaker.VarDef(modifiers, varName, type, versionExp);
    }

    private JCTree.JCVariableDecl createFieldDefinitionsVariable(JCTree.JCClassDecl jcClassDecl,
                                                                 Map<Name, JCTree.JCVariableDecl> variableDeclMap, java.util.List<Map.Entry<Name, Integer>> sortedNameIndexes) {        // private static final FieldDefinition[] fieldDefinitions = ...;
        final JCTree.JCModifiers modifiers = treeMaker.Modifiers(Flags.PRIVATE | Flags.STATIC | Flags.FINAL);
        final Name variableName = names.fromString(VARIABLE_FIELD_DEFINITIONS);

        Symbol.ClassSymbol classSymbol = new Symbol.ClassSymbol(0, names.fromString(FieldDefinition.class.getSimpleName()), null);
        JCTree.JCExpression elementType = treeMaker.Ident(classSymbol);       // treeMaker.ClassLiteral(classSymbol); -> FieldDefinition.class
        JCTree.JCArrayTypeTree arrayTypeTree = treeMaker.TypeArray(elementType);

        java.util.List<FieldDefinition> fieldDefinitionList = new ArrayList<>(sortedNameIndexes.size());
        ListBuffer<JCTree.JCExpression> elements = new ListBuffer<>();
        for (Map.Entry<Name, Integer> entry : sortedNameIndexes) {
            Name name = entry.getKey();
            Integer index = entry.getValue();
            JCTree.JCVariableDecl var = variableDeclMap.get(name);
            FieldType fieldType = FieldType.valueOfDef(var.vartype.toString());
            ListBuffer<JCTree.JCExpression> argValues = new ListBuffer<>();
            FieldDefinition fieldDefinition = new FieldDefinition(name.toString(), index, fieldType, !(var.vartype instanceof JCTree.JCPrimitiveTypeTree));
            fieldDefinitionList.add(fieldDefinition);

            argValues.add(treeMaker.Literal(fieldDefinition.getFixedName()));
            argValues.add(treeMaker.Literal(TypeTag.INT, fieldDefinition.getIndex()));
            argValues.add(treeMaker.Select(treeMaker.Ident(names.fromString(FieldType.class.getSimpleName())), names.fromString(fieldDefinition.getType().name())));
            argValues.add(treeMaker.Literal(fieldDefinition.isNullable()));
            JCTree.JCNewClass newClass = treeMaker.NewClass(null, List.nil(), elementType, argValues.toList(), null);
            elements.add(newClass);
        }

        printVersionableSchema(jcClassDecl, fieldDefinitionList);

        JCTree.JCNewArray newArray = treeMaker.NewArray(elementType, List.nil(), elements.toList());
        return treeMaker.VarDef(modifiers, variableName, arrayTypeTree, newArray);
    }

    private JCTree.JCAnnotation getAnnotation(JCTree.JCModifiers modifiers, Class<? extends Annotation> clazz) {
        List<JCTree.JCAnnotation> annotations = modifiers.getAnnotations();
        for (JCTree.JCAnnotation annotation : annotations) {
            if (annotation.getAnnotationType().type.toString().equals(clazz.getCanonicalName())) {
                return annotation;
            }
        }
        return null;
    }

    private JCTree.JCExpression getAnnotationAttribute(JCTree.JCAnnotation annotation, String attribute) {
        List<JCTree.JCExpression> args = annotation.getArguments();
        for (JCTree.JCExpression expression : args) {
            if (expression.hasTag(JCTree.Tag.ASSIGN)) {
                JCTree.JCAssign assign = (JCTree.JCAssign) expression;
                if (assign.lhs.hasTag(JCTree.Tag.IDENT) && ((JCTree.JCIdent) assign.lhs).getName().contentEquals(attribute)) {
                    return assign.rhs;
                }
            }
        }
        return null;
    }

    private NameIndexPair createNameIndexPair(Map.Entry<Name, JCTree.JCVariableDecl> entry) {
        JCTree.JCAnnotation annotation = getAnnotation(entry.getValue().getModifiers(), PField.class);
        if (annotation == null) {
            return null;
        }

        JCTree.JCExpression valueExp = getAnnotationAttribute(annotation, "value");
        return valueExp != null ? new NameIndexPair(entry.getKey(), Integer.valueOf(valueExp.toString())) : null;
    }

    private Name methodName(Name varName, String prefix) {
        String name = varName.toString();
        StringBuffer buffer = new StringBuffer(prefix.length() + name.length());
        buffer.append(prefix);
        buffer.append(name.substring(0, 1).toUpperCase());
        buffer.append(name.substring(1));
        return names.fromString(buffer.toString());
    }

    private JCTree.JCVariableDecl numberVariable(String name, TypeTag tag, JCTree.JCExpression defaultValue, boolean isStatic) {
        final JCTree.JCModifiers modifiers = isStatic ?
                treeMaker.Modifiers(Flags.PRIVATE | Flags.STATIC | Flags.FINAL) : treeMaker.Modifiers(Flags.PRIVATE);
        Name variableName = names.fromString(name);
        return treeMaker.VarDef(modifiers, variableName, treeMaker.TypeIdent(tag), defaultValue);
    }

    private List<JCTree> generateRouteGetterMethods(Map<Name, JCTree.JCVariableDecl> variableDeclMap, Map<Name, Integer> indexMap) {
        final JCTree.JCModifiers modifiers = treeMaker.Modifiers(Flags.PUBLIC);
        final JCTree.JCVariableDecl paramVar = treeMaker.VarDef(
                treeMaker.Modifiers(Flags.PARAMETER), names.fromString(GETTER_PARAM_INDEX), treeMaker.TypeIdent(TypeTag.INT), null);
        final JCTree.JCExpression jcThrow = treeMaker.Ident(names.fromString(IllegalStateException.class.getSimpleName()));
        final List<JCTree.JCVariableDecl> params = List.of(paramVar);
        final List<JCTree.JCExpression> jcThrows = List.of(jcThrow);
        ListBuffer<JCTree> methodDecls = new ListBuffer<>();
        METHOD_TYPE_MAP.forEach((name, type) -> {
            Name methodName = names.fromString(name);
            JCTree.JCExpression returnType = type instanceof TypeTag ? treeMaker.TypeIdent((TypeTag) type) : treeMaker.Ident(names.fromString(type.toString()));
            ListBuffer<JCTree.JCStatement> jcStatements = new ListBuffer<>();
            ListBuffer<JCTree.JCCase> cases = new ListBuffer<>();
            String typeName = type instanceof TypeTag ? type.toString().toLowerCase() : (String) type;
            for (Map.Entry<Name, Integer> entry : indexMap.entrySet()) {
                JCTree.JCVariableDecl variableDecl = variableDeclMap.get(entry.getKey());
                if (variableDecl.vartype.toString().equals(typeName)) {
                    ListBuffer<JCTree.JCStatement> caseStatements = new ListBuffer<>();
                    caseStatements.append(treeMaker.Return(treeMaker.Select(treeMaker.Ident(names.fromString(THIS)), variableDecl.name)));
                    cases.add(treeMaker.Case(treeMaker.Literal(TypeTag.INT, entry.getValue()), caseStatements.toList()));
                }
            }

            if (cases.nonEmpty()) {
                jcStatements.append(treeMaker.Switch(treeMaker.Ident(names.fromString(GETTER_PARAM_INDEX)), cases.toList()));
            }

            JCTree.JCExpression messageExp = treeMaker.Apply(
                    List.nil(),
                    treeMaker.Select(
                            treeMaker.Ident(names.fromString("String")),
                            names.fromString("format")
                    ),
                    List.of(
                            treeMaker.Literal("index %s is not typeof %s"),
                            treeMaker.Ident(names.fromString(GETTER_PARAM_INDEX)),
                            treeMaker.Literal(type.toString())
                    )
            );
            jcStatements.append(treeMaker.Throw(treeMaker.NewClass(null, List.nil(), jcThrow, List.of(messageExp), null)));
            JCTree.JCBlock block = treeMaker.Block(0, jcStatements.toList());
            JCTree.JCMethodDecl methodDecl =
                    treeMaker.MethodDef(modifiers, methodName, returnType, List.nil(), params, jcThrows, block, null);
            methodDecls.append(methodDecl);
        });

        return methodDecls.toList();
    }

    private JCTree.JCMethodDecl generateGetterStaticMethod(JCTree.JCClassDecl classDecl, JCTree.JCVariableDecl variableDecl) {
        final JCTree.JCModifiers modifiers = treeMaker.Modifiers(Flags.PUBLIC);
        final Name methodName = methodName(variableDecl.name, Objects.equals(variableDecl.vartype.toString(), "boolean") ? "is" : GET);

        ListBuffer<JCTree.JCStatement> jcStatements = new ListBuffer<>();
        jcStatements.append(treeMaker.Return(treeMaker.Select(treeMaker.Ident(classDecl.name), variableDecl.name)));
        final JCTree.JCBlock block = treeMaker.Block(0, jcStatements.toList());

        return treeMaker.MethodDef(modifiers, methodName, variableDecl.vartype, List.nil(), List.nil(), List.nil(), block,
                null);
    }

    private JCTree.JCMethodDecl generateSetterMethod(JCTree.JCVariableDecl variableDecl, int index, boolean checkNull) throws ClassNotFoundException,
            IllegalAccessException, InstantiationException {
        final JCTree.JCModifiers modifiers = treeMaker.Modifiers(Flags.PUBLIC);
        final Name varName = variableDecl.getName();
        Name methodName = methodName(varName, SET);

        ListBuffer<JCTree.JCStatement> jcStatements = new ListBuffer<>();
        jcStatements.append(treeMaker.Exec(treeMaker.Assign(
                treeMaker.Select(treeMaker.Ident(names.fromString(THIS)), varName),
                treeMaker.Ident(varName)
        )));
        jcStatements.append(generateSetMaskAssignment(variableDecl, index, checkNull));
        final JCTree.JCBlock block = treeMaker.Block(0, jcStatements.toList());

        JCTree.JCExpression returnType =
                treeMaker.Type((Type) (Class.forName("com.sun.tools.javac.code.Type$JCVoidType").newInstance()));
        List<JCTree.JCTypeParameter> typeParameters = List.nil();

        final JCTree.JCVariableDecl paramVars = treeMaker.VarDef(treeMaker.Modifiers(Flags.PARAMETER,
                List.nil()), variableDecl.name, variableDecl.vartype, null);
        final List<JCTree.JCVariableDecl> params = List.of(paramVars);

        List<JCTree.JCExpression> throwClauses = List.nil();
        // 重新构造一个方法, 最后一个参数是方法注解的默认值，这里没有
        return treeMaker.MethodDef(modifiers, methodName, returnType, typeParameters, params, throwClauses, block,
                null);
    }

    private JCTree.JCMethodDecl generateGetterMethod(JCTree.JCVariableDecl variableDecl, TypeTag typeTag) {
        final JCTree.JCModifiers modifiers = treeMaker.Modifiers(Flags.PUBLIC);
        final Name varName = variableDecl.getName();
        Name methodName = methodName(varName, GET);

        ListBuffer<JCTree.JCStatement> jcStatements = new ListBuffer<>();
        jcStatements.append(treeMaker.Return(
                treeMaker.Select(treeMaker.Ident(names.fromString(THIS)), varName)
        ));
        final JCTree.JCBlock block = treeMaker.Block(0, jcStatements.toList());

        JCTree.JCExpression returnType = treeMaker.TypeIdent(typeTag);
        List<JCTree.JCExpression> throwClauses = List.nil();
        // 重新构造一个方法, 最后一个参数是方法注解的默认值，这里没有
        return treeMaker.MethodDef(modifiers, methodName, returnType, List.nil(), List.nil(), throwClauses, block,
                null);
    }

    private JCTree.JCMethodDecl generateClearMethod() throws ClassNotFoundException,
            IllegalAccessException, InstantiationException {                    // public void clearAllMask() { this._$setMask = 0; this._$unsetMask = 0; }
        final JCTree.JCModifiers modifiers = treeMaker.Modifiers(Flags.PUBLIC);
        final Name methodName = names.fromString(METHOD_CLEAR_ALL_MASK);

        ListBuffer<JCTree.JCStatement> jcStatements = new ListBuffer<>();
        jcStatements.append(treeMaker.Exec(treeMaker.Assign(
                treeMaker.Select(treeMaker.Ident(names.fromString(THIS)), names.fromString(VARIABLE_SET_MASK)),
                treeMaker.Literal(TypeTag.INT, 0)
        )));
        jcStatements.append(treeMaker.Exec(treeMaker.Assign(
                treeMaker.Select(treeMaker.Ident(names.fromString(THIS)), names.fromString(VARIABLE_UNSET_MASK)),
                treeMaker.Literal(TypeTag.INT, 0)
        )));
        final JCTree.JCBlock block = treeMaker.Block(0, jcStatements.toList());

        JCTree.JCExpression returnType =
                treeMaker.Type((Type) (Class.forName("com.sun.tools.javac.code.Type$JCVoidType").newInstance()));
        List<JCTree.JCTypeParameter> typeParameters = List.nil();

        final List<JCTree.JCVariableDecl> params = List.nil();
        List<JCTree.JCExpression> throwClauses = List.nil();
        // 重新构造一个方法, 最后一个参数是方法注解的默认值，这里没有
        return treeMaker.MethodDef(modifiers, methodName, returnType, typeParameters, params, throwClauses, block,
                null);
    }

    private void appendMaskAssignmentToMethod(JCTree.JCVariableDecl variableDecl, JCTree.JCMethodDecl jcMethodDecl, int index, boolean checkNull) {
        final Context context = ((JavacProcessingEnvironment) processingEnv).getContext();
        final JavacElements elementUtils = (JavacElements) processingEnv.getElementUtils();
        final TreeMaker treeMaker = TreeMaker.instance(context);

        treeMaker.pos = jcMethodDecl.pos;
        jcMethodDecl.body = treeMaker.Block(0, List.of(
                jcMethodDecl.body,
                generateSetMaskAssignment(variableDecl, index, checkNull)
        ));
    }

    private JCTree.JCStatement generateSetMaskAssignment(JCTree.JCVariableDecl variableDecl, int index, boolean checkNull) {
        if (checkNull) {
            return treeMaker.If(
                    treeMaker.Binary(JCTree.Tag.NE, treeMaker.Ident(variableDecl.getName()), treeMaker.Literal(TypeTag.BOT, null)),
                    treeMaker.Block(0, List.of(
                            generateMaskAssignment(VARIABLE_SET_MASK, index),               // this._$setMask |= 2;
                            generateMaskClear(VARIABLE_UNSET_MASK, index)                   // this._$unsetMask &= -3;
                    )),
                    treeMaker.Block(0, List.of(
                            generateMaskAssignment(VARIABLE_UNSET_MASK, index),             // this._$unsetMask |= 2;
                            generateMaskClear(VARIABLE_SET_MASK, index)                     // this._$unsetMask &= -3;
                    ))
            );
        } else {
            return generateMaskAssignment(VARIABLE_SET_MASK, index);                        // this._$setMask |= 1;
        }
    }

    private JCTree.JCExpressionStatement generateMaskAssignment(String fieldName, int index) {
        return treeMaker.Exec(
                treeMaker.Assignop(
                        JCTree.Tag.BITOR_ASG,
                        treeMaker.Select(
                                treeMaker.Ident(names.fromString(THIS)),
                                names.fromString(fieldName)
                        ),
                        treeMaker.Binary(JCTree.JCBinary.Tag.SL, oneValue,
                                treeMaker.Literal(index)))
        );
    }

    private JCTree.JCExpressionStatement generateMaskClear(String fieldName, int index) {
        return treeMaker.Exec(
                treeMaker.Assignop(
                        JCTree.Tag.BITAND_ASG,
                        treeMaker.Select(
                                treeMaker.Ident(names.fromString(THIS)),
                                names.fromString(fieldName)
                        ),
                        treeMaker.Unary(JCTree.Tag.COMPL,
                                treeMaker.Binary(JCTree.JCBinary.Tag.SL, oneValue, treeMaker.Literal(index)))
                )
        );
    }

    private void printVersionableSchema(JCTree.JCClassDecl jcClassDecl, java.util.List<FieldDefinition> fieldDefinitionList) {
        JCTree.JCAnnotation annotation = getAnnotation(jcClassDecl.getModifiers(), PRecord.class);
        JCTree.JCExpression nameExp = getAnnotationAttribute(annotation, "name");
        JCTree.JCExpression idExp = getAnnotationAttribute(annotation, "id");
        JCTree.JCExpression versionExp = getAnnotationAttribute(annotation, "version");
        JCTree.JCExpression sortingExp = getAnnotationAttribute(annotation, "sortingOrder");

        FieldDefinition[] fieldDefinitions = new FieldDefinition[fieldDefinitionList.size()];
        fieldDefinitionList.toArray(fieldDefinitions);

        String name = nameExp.toString();
        byte id = Byte.parseByte(idExp.toString());
        byte version = Byte.parseByte(versionExp.toString());
        boolean sortingOrder = sortingExp != null ? Boolean.parseBoolean(sortingExp.toString()) : false;
        VersionableSchema schema = new VersionableSchema(name.substring(1, name.length() - 1), id, version, sortingOrder, fieldDefinitions);

        String filePath = "schemas/" + schema.getName() + "_" + schema.getVersion() + ".json";
        if (!new File(".").getName().equals("db-buffer")) {
            filePath = "../" + filePath;
        }
        File dir = new File(filePath).getParentFile();
        messager.printMessage(Diagnostic.Kind.NOTE,dir.getAbsolutePath());
        if (!dir.exists()) {
            dir.mkdirs();
        }
        try (PrintWriter printWriter = new PrintWriter(new FileWriter(filePath))) {
            printWriter.write(JSON.toJSONString(schema, true));
            messager.printMessage(Diagnostic.Kind.NOTE, String.format("print schema file %s", filePath));
        } catch (IOException exception) {
            messager.printMessage(Diagnostic.Kind.ERROR, String.format("Schema print failure %s", exception.getMessage()));
        }
    }

    private static <T> T jbUnwrap(Class<? extends T> iface, T wrapper) {
        T unwrapped = null;
        try {
            final Class<?> apiWrappers = wrapper.getClass().getClassLoader().loadClass("org.jetbrains.jps.javac.APIWrappers");
            final Method unwrapMethod = apiWrappers.getDeclaredMethod("unwrap", Class.class, Object.class);
            unwrapped = iface.cast(unwrapMethod.invoke(null, iface, wrapper));
        }
        catch (Throwable ignored) {}
        return unwrapped != null? unwrapped : wrapper;
    }

    @Getter
    @AllArgsConstructor
    class NameIndexPair {

        private Name name;

        private Integer index;
    }
}
