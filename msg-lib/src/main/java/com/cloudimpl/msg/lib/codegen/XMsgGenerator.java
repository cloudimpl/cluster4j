/*
 * Copyright 2021 nuwan.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudimpl.msg.lib.codegen;

import com.cloudimpl.codegen4j.AccessLevel;
import com.cloudimpl.codegen4j.ClassBlock;
import com.cloudimpl.codegen4j.ClassBuilder;
import com.cloudimpl.codegen4j.ConditionalBlock;
import com.cloudimpl.codegen4j.ConstructorBlock;
import com.cloudimpl.codegen4j.FunctionBlock;
import com.cloudimpl.codegen4j.JavaFile;
import com.cloudimpl.codegen4j.StaticBlock;
import com.cloudimpl.codegen4j.SynchronousBlock;
import com.cloudimpl.codegen4j.spi.MavenCodeGenSpi;
import com.cloudimpl.msg.lib.JsonMsg;
import com.cloudimpl.msg.lib.XMsgException;
import com.cloudimpl.msg.lib.XMsgTemplate;
import com.cloudimpl.msg.lib.ZCharSequence;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.green.jelly.JsonNumber;

/**
 *
 * @author nuwan
 */
public class XMsgGenerator extends MavenCodeGenSpi {

    public XMsgGenerator() {
        super("xmgenerator", "com.cloudimpl.cluster4j");
    }

    @Override
    public void execute() {
        compileFiles(getAllSourceFiles().stream().map(s -> new File(s)).collect(Collectors.toList()));
        loadMsgTemplates().forEach(this::generateMsg);
    }

    public List<Class<?>> loadMsgTemplates() {
        ScanResult rs = new ClassGraph().enableClassInfo().scan();
        ClassInfoList list = rs.getClassesImplementing(XMsgTemplate.class.getName());
        return list.loadClasses().stream()
                .filter(cls->cls.getProtectionDomain().getCodeSource().getLocation().getPath().startsWith(getProject().getBuildDir()))
                .collect(Collectors.toList());
    }

    /*
    static
    {
        mapTemplates.put(TestMsg.class.getName(),()->new TestMsg());
    }
     */
    private void generateMsg(Class<?> cls) {

        if (!cls.getSimpleName().startsWith("_")) {
            throw new XMsgGenException("msg template names should start with _");
        }
        ClassBuilder clsBuilder = new ClassBuilder();
        ClassBlock clsBlock = clsBuilder.createClass(cls.getSimpleName().substring(1)).withPackageName(cls.getPackageName())
                .extend(JsonMsg.class.getSimpleName())
                .withImports(JsonMsg.class.getName(),JsonNumber.class.getName(), ZCharSequence.class.getName())
                .withAccess(AccessLevel.PUBLIC);
        ConstructorBlock ctrBlock = clsBlock.createConstructor();
        
        StaticBlock staticBlock = clsBlock.createStaticBlock();
        SynchronousBlock synBlock = staticBlock.createSynchronousBlock("mapTemplates");
        synBlock.stmt().append("mapTemplates.put(new ZCharSequence(" + cls.getSimpleName().substring(1) + ".class.getName()),()->new " + cls.getSimpleName().substring(1) + "())").end();
        List<Field> flds = Arrays.asList(cls.getDeclaredFields());
        flds.stream().forEach(fld -> {
            Class<?> fldType = getType(fld);
            if (fldType == ZCharSequence.class) {
                clsBlock.var(fldType.getSimpleName(), fld.getName()).withAccess(AccessLevel.PRIVATE).withFinal().end();
                ctrBlock.stmt().append("this."+fld.getName()+" = new ZCharSequence()").end();
            }else
            {
                clsBlock.var(fldType.getSimpleName(), fld.getName()).withAccess(AccessLevel.PRIVATE).end();
            }

        });
        generateJsonNumberSetter(clsBlock, flds, cls);
        generateStringSetter(clsBlock, flds, cls);
        generateReset(clsBlock, flds, cls);
        JavaFile file = JavaFile.wrap(clsBlock);
        file.writeTo(new File(getCodeGenFolder()));
    }

    private void generateJsonNumberSetter(ClassBlock clsBlock, List<Field> fields, Class<?> template) {
        FunctionBlock fblock = clsBlock.createFunction("set")
                .withReturnType("boolean")
                .withArgs("CharSequence name, JsonNumber number")
                .withAccess(AccessLevel.PROTECTED)
                .withAnnotation(Override.class.getSimpleName());

        ConditionalBlock ifBlock = null;
        for (Field f : fields) {
            if (ifBlock == null) {
                ifBlock = fblock.createIf("CharSequence.compare(name,\"" + f.getName() + "\") == 0");
            } else {
                ifBlock = fblock.createElseIf("CharSequence.compare(name,\"" + f.getName() + "\") == 0");
            }
            if (isNumber(f)) {

                if (isLong(f)) {
                    ifBlock.stmt().append("this." + f.getName() + " = number.mantissa()").end();
                } else if (isPrimitive(f)) {
                    ifBlock.stmt().append("this." + f.getName() + " = " + "(" + f.getType().getSimpleName() + ")number.mantissa()").end();
                }

            } else {
                ifBlock.withReturnStatment("false").end();
            }
            
        }
        fblock.withReturnStatment("true").end();
    }

    private void generateStringSetter(ClassBlock clsBlock, List<Field> fields, Class<?> template) {
        FunctionBlock fblock = clsBlock.createFunction("set")
                .withReturnType("boolean")
                .withArgs("CharSequence name, CharSequence value")
                .withAccess(AccessLevel.PROTECTED)
                .withAnnotation(Override.class.getSimpleName());
        ConditionalBlock ifBlock = null;
        for (Field f : fields) {
            if (ifBlock == null) {
                ifBlock = fblock.createIf("CharSequence.compare(name,\"" + f.getName() + "\") == 0");
            } else {
                ifBlock = fblock.createElseIf("CharSequence.compare(name,\"" + f.getName() + "\") == 0");
            }
            if (getType(f) == ZCharSequence.class) {
                ifBlock.stmt().append("this." + f.getName() + ".set(value)").end();
            } else {
                ifBlock.withReturnStatment("false").end();
            }
        }
        fblock.withReturnStatment("true").end();
    }

    private void generateReset(ClassBlock clsBlock, List<Field> fields, Class<?> template) {
        FunctionBlock fblock = clsBlock.createFunction("reset")
                .withAccess(AccessLevel.PUBLIC)
                .withAnnotation(Override.class.getSimpleName());

        fields.forEach(fld -> {
            if (isNumber(fld)) {
                fblock.stmt().append("this." + fld.getName() + " = 0").end();
            } else {
                fblock.stmt().append("this." + fld.getName() + ".reset()").end();
            }
        });

    }

    private boolean isNumber(Field f) {
        Class<?> t = getType(f);
        return t == short.class || t == int.class || t == long.class;
    }

    private boolean isPrimitive(Field f) {
        Class<?> t = getType(f);
        return t == short.class || t == int.class || t == long.class;
    }

    private boolean isLong(Field f) {
        return getType(f) == long.class;
    }

    private Class<?> getType(Field f) {
        Class<?> type = f.getType();
        if (type == String.class) {
            return ZCharSequence.class;
        } else if (type == short.class || type == int.class || type == long.class) {
            return f.getType();
        } else {
            throw new XMsgGenException("unsupported data type : " + type.getName());
        }
    }
}
