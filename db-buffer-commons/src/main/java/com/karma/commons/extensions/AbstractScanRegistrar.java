package com.karma.commons.extensions;

import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.ResourceLoaderAware;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class AbstractScanRegistrar implements ImportBeanDefinitionRegistrar, ResourceLoaderAware, EnvironmentAware {

    protected ResourceLoader resourceLoader;

    protected Environment environment;

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void setResourceLoader(ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @Override
    public abstract void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry);

    protected ClassPathScanningCandidateComponentProvider getScanner() {

        return new ClassPathScanningCandidateComponentProvider(false, this.environment) {
            @Override
            protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {

                AnnotationMetadata annotationMetadata = beanDefinition.getMetadata();
                return annotationMetadata.isConcrete() && ! annotationMetadata.isAnnotation();
            }
        };
    }

    protected String getBasePath(Map<String, Object> attributes) {

        String basePath = (String) attributes.get("configPath");
        if (StringUtils.isEmpty(basePath)) {
            return null;
        } else {
            basePath = basePath.startsWith(File.separator) ? basePath : File.separator + basePath;
            basePath = basePath.endsWith(File.separator) ? basePath : basePath + File.separator;
            return basePath;
        }
    }

    protected Set<String> getBasePackages(Map<String, Object> attributes, String basePackageName) {

        Set<String> basePackages = new HashSet<>();

        for (String pkg : (String[]) attributes.get("basePackages")) {
            if (StringUtils.hasText(pkg)) {
                basePackages.add(pkg);
            }
        }
        for (Class<?> clazz : (Class[]) attributes.get("basePackageClasses")) {
            basePackages.add(ClassUtils.getPackageName(clazz));
        }

        if (basePackages.isEmpty()) {
            basePackages.add(basePackageName);
        }
        return basePackages;
    }
}
