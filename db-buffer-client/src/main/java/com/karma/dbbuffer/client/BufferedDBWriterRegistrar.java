package com.karma.dbbuffer.client;

import com.karma.commons.extensions.AbstractScanRegistrar;
import com.karma.dbbuffer.client.config.DBBufferWriterConfig;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.core.type.AnnotationMetadata;

public class BufferedDBWriterRegistrar extends AbstractScanRegistrar {

    private static final String SERVICE_BEAN_NAME = "bufferedDBWriter";

    private static final String CONFIGURATION_BEAN_NAME = "dbBufferWriterConfig";

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        if (!registry.containsBeanDefinition(SERVICE_BEAN_NAME)) {
            GenericBeanDefinition configDefinition = new GenericBeanDefinition();
            configDefinition.setBeanClass(DBBufferWriterConfig.class);
            registry.registerBeanDefinition(CONFIGURATION_BEAN_NAME, configDefinition);

            GenericBeanDefinition serviceDefinition = new GenericBeanDefinition();
            serviceDefinition.setBeanClass(BufferedDBWriter.class);
            registry.registerBeanDefinition(SERVICE_BEAN_NAME, serviceDefinition);
        }
    }
}
