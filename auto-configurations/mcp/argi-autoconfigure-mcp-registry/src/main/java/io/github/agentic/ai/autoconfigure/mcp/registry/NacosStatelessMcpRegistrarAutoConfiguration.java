/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.agentic.ai.autoconfigure.mcp.registry;

import io.github.agentic.ai.mcp.nacos.NacosMcpProperties;
import io.github.agentic.ai.mcp.nacos.service.NacosMcpOperationService;
import io.github.agentic.ai.mcp.registry.NacosMcpRegistryProperties;
import io.github.agentic.ai.mcp.registry.NacosStatelessMcpRegistrar;
import com.alibaba.nacos.api.ai.constant.AiConstants;
import com.alibaba.nacos.api.exception.NacosException;
import io.modelcontextprotocol.server.McpStatelessAsyncServer;
import io.modelcontextprotocol.server.McpStatelessSyncServer;
import org.springframework.ai.mcp.server.common.autoconfigure.McpServerAutoConfiguration;
import org.springframework.ai.mcp.server.common.autoconfigure.McpServerStatelessAutoConfiguration;
import org.springframework.ai.mcp.server.common.autoconfigure.McpServerStdioDisabledCondition;
import org.springframework.ai.mcp.server.common.autoconfigure.properties.McpServerProperties;
import org.springframework.ai.mcp.server.common.autoconfigure.properties.McpServerSseProperties;
import org.springframework.ai.mcp.server.common.autoconfigure.properties.McpServerStreamableHttpProperties;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;

import java.lang.reflect.Field;
import java.util.Properties;


@EnableConfigurationProperties({ NacosMcpRegistryProperties.class, NacosMcpProperties.class,
        McpServerProperties.class, McpServerSseProperties.class, McpServerStreamableHttpProperties.class})
@AutoConfiguration(after = McpServerAutoConfiguration.class)
@ConditionalOnProperty(prefix = NacosMcpRegistryProperties.CONFIG_PREFIX, name = "enabled", havingValue = "true",
        matchIfMissing = false)
@Conditional({ McpServerStdioDisabledCondition.class,
        McpServerStatelessAutoConfiguration.EnabledStatelessServerCondition.class })
public class NacosStatelessMcpRegistrarAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(NacosMcpOperationService.class)
    public NacosMcpOperationService nacosMcpOperationService(NacosMcpProperties nacosMcpProperties) {
        Properties nacosProperties = nacosMcpProperties.getNacosProperties();
        try {
            return new NacosMcpOperationService(nacosProperties);
        }
        catch (NacosException e) {
            throw new RuntimeException(e);
        }
    }

    @Bean
    @ConditionalOnProperty(prefix = McpServerProperties.CONFIG_PREFIX, name = "type", havingValue = "SYNC",
            matchIfMissing = true)
    public NacosStatelessMcpRegistrar nacosStatelessMcpRegistrarSync(NacosMcpOperationService nacosMcpOperationService,
            McpStatelessSyncServer mcpStatelessSyncServer, NacosMcpProperties nacosMcpProperties,
            NacosMcpRegistryProperties nacosMcpRegistryProperties, McpServerProperties mcpServerProperties, McpServerSseProperties mcpServerSseProperties,
            ApplicationContext applicationContext)
            throws NoSuchFieldException, IllegalAccessException {
        Field asyncServerField = McpStatelessSyncServer.class.getDeclaredField("asyncServer");
        asyncServerField.setAccessible(true);
        McpStatelessAsyncServer mcpStatelessAsyncServer = (McpStatelessAsyncServer) asyncServerField.get(mcpStatelessSyncServer);
        return getNacosMcpRegistrar(nacosMcpOperationService, mcpStatelessAsyncServer, nacosMcpProperties,
                nacosMcpRegistryProperties, mcpServerProperties, mcpServerSseProperties, applicationContext);
    }

    @Bean
    //	@ConditionalOnBean(McpAsyncServer.class)
    @ConditionalOnProperty(prefix = McpServerProperties.CONFIG_PREFIX, name = "type", havingValue = "ASYNC")
    public NacosStatelessMcpRegistrar nacosStatelessMcpRegistrarAsync(NacosMcpOperationService nacosMcpOperationService,
            McpStatelessAsyncServer mcpStatelessAsyncServer, NacosMcpProperties nacosMcpProperties,
            NacosMcpRegistryProperties nacosMcpRegistryProperties, McpServerProperties mcpServerProperties, McpServerSseProperties mcpServerSseProperties,
            ApplicationContext applicationContext) {
        return getNacosMcpRegistrar(nacosMcpOperationService, mcpStatelessAsyncServer, nacosMcpProperties,
                nacosMcpRegistryProperties, mcpServerProperties, mcpServerSseProperties, applicationContext);
    }


    private NacosStatelessMcpRegistrar getNacosMcpRegistrar(NacosMcpOperationService nacosMcpOperationService,
            McpStatelessAsyncServer mcpStatelessAsyncServer, NacosMcpProperties nacosMcpProperties,
            NacosMcpRegistryProperties nacosMcpRegistryProperties, McpServerProperties mcpServerProperties, McpServerSseProperties mcpServerSseProperties,
            ApplicationContext applicationContext) {
        return new NacosStatelessMcpRegistrar(nacosMcpOperationService, mcpStatelessAsyncServer, nacosMcpProperties, nacosMcpRegistryProperties,
                mcpServerProperties, mcpServerSseProperties, applicationContext ,AiConstants.Mcp.MCP_PROTOCOL_STREAMABLE);
    }

}
