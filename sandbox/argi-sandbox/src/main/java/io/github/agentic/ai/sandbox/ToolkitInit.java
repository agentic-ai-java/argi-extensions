/*
 * Copyright 2024-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.agentic.ai.sandbox;

import io.github.agentic.ai.sandbox.tools.base.ArgiBasePythonRunner;
import io.github.agentic.ai.sandbox.tools.base.ArgiBaseShellRunner;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserBackNavigator;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserClicker;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserCloser;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserConsoleMessagesRetriever;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserDialogHandler;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserDragger;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserFileUploader;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserForwardNavigator;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserHoverer;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserKeyPresser;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserNavigator;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserNetworkRequestsRetriever;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserOptionSelector;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserPdfSaver;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserScreenshotTaker;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserSnapshotTaker;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserTabCloser;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserTabCreator;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserTabLister;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserTabSelector;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserTyper;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserWaiter;
import io.github.agentic.ai.sandbox.tools.browser.ArgiBrowserWindowResizer;
import io.github.agentic.ai.sandbox.tools.fs.ArgiFsAllowedDirectoriesLister;
import io.github.agentic.ai.sandbox.tools.fs.ArgiFsDirectoryCreator;
import io.github.agentic.ai.sandbox.tools.fs.ArgiFsDirectoryLister;
import io.github.agentic.ai.sandbox.tools.fs.ArgiFsFileEditor;
import io.github.agentic.ai.sandbox.tools.fs.ArgiFsFileInfoRetriever;
import io.github.agentic.ai.sandbox.tools.fs.ArgiFsFileMover;
import io.github.agentic.ai.sandbox.tools.fs.ArgiFsFileReader;
import io.github.agentic.ai.sandbox.tools.fs.ArgiFsFileSearcher;
import io.github.agentic.ai.sandbox.tools.fs.ArgiFsFileWriter;
import io.github.agentic.ai.sandbox.tools.fs.ArgiFsMultiFileReader;
import io.github.agentic.ai.sandbox.tools.fs.ArgiFsTreeBuilder;
import io.github.agentic.ai.sandbox.tools.mcp.ArgiMCPTool;
import io.agentscope.runtime.sandbox.box.Sandbox;
import io.agentscope.runtime.sandbox.manager.SandboxService;
import io.agentscope.runtime.sandbox.tools.MCPTool;
import io.agentscope.runtime.sandbox.tools.McpConfigConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.ToolCallback;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ToolkitInit {
    public static Logger logger = LoggerFactory.getLogger(ToolkitInit.class);

    public static List<ToolCallback> getAllTools(Sandbox sandbox) {
        return List.of(
                RunPythonCodeTool(sandbox),
                RunShellCommandTool(sandbox),
                ReadFileTool(sandbox),
                ReadMultipleFilesTool(sandbox),
                WriteFileTool(sandbox),
                EditFileTool(sandbox),
                CreateDirectoryTool(sandbox),
                ListDirectoryTool(sandbox),
                DirectoryTreeTool(sandbox),
                MoveFileTool(sandbox),
                SearchFilesTool(sandbox),
                GetFileInfoTool(sandbox),
                ListAllowedDirectoriesTool(sandbox),
                BrowserNavigateTool(sandbox),
                BrowserClickTool(sandbox),
                BrowserTypeTool(sandbox),
                BrowserTakeScreenshotTool(sandbox),
                BrowserSnapshotTool(sandbox),
                BrowserTabNewTool(sandbox),
                BrowserTabSelectTool(sandbox),
                BrowserTabCloseTool(sandbox),
                BrowserWaitForTool(sandbox),
                BrowserResizeTool(sandbox),
                BrowserCloseTool(sandbox),
                BrowserConsoleMessagesTool(sandbox),
                BrowserHandleDialogTool(sandbox),
                BrowserFileUploadTool(sandbox),
                BrowserPressKeyTool(sandbox),
                BrowserNavigateBackTool(sandbox),
                BrowserNavigateForwardTool(sandbox),
                BrowserNetworkRequestsTool(sandbox),
                BrowserPdfSaveTool(sandbox),
                BrowserDragTool(sandbox),
                BrowserHoverTool(sandbox),
                BrowserSelectOptionTool(sandbox),
                BrowserTabListTool(sandbox)
        );
    }

    // Base tools
    public static ToolCallback RunPythonCodeTool(Sandbox sandbox) {
        ArgiBasePythonRunner argiBasePythonRunner = new ArgiBasePythonRunner();
        argiBasePythonRunner.setSandbox(sandbox);
        return argiBasePythonRunner.buildTool();
    }

    public static ToolCallback RunShellCommandTool(Sandbox sandbox) {
        ArgiBaseShellRunner argiBaseShellRunner = new ArgiBaseShellRunner();
        argiBaseShellRunner.setSandbox(sandbox);
        return argiBaseShellRunner.buildTool();
    }

    // Browser tools
    public static ToolCallback BrowserNavigateTool(Sandbox sandbox) {
        ArgiBrowserNavigator argiBrowserNavigator = new ArgiBrowserNavigator();
        argiBrowserNavigator.setSandbox(sandbox);
        return argiBrowserNavigator.buildTool();
    }

    public static ToolCallback BrowserClickTool(Sandbox sandbox) {
        ArgiBrowserClicker argiBrowserClicker = new ArgiBrowserClicker();
        argiBrowserClicker.setSandbox(sandbox);
        return argiBrowserClicker.buildTool();
    }

    public static ToolCallback BrowserTypeTool(Sandbox sandbox) {
        ArgiBrowserTyper argiBrowserTyper = new ArgiBrowserTyper();
        argiBrowserTyper.setSandbox(sandbox);
        return argiBrowserTyper.buildTool();
    }

    public static ToolCallback BrowserSnapshotTool(Sandbox sandbox) {
        ArgiBrowserSnapshotTaker argiBrowserSnapshotTaker = new ArgiBrowserSnapshotTaker();
        argiBrowserSnapshotTaker.setSandbox(sandbox);
        return argiBrowserSnapshotTaker.buildTool();
    }

    public static ToolCallback BrowserTakeScreenshotTool(Sandbox sandbox) {
        ArgiBrowserScreenshotTaker argiBrowserScreenshotTaker = new ArgiBrowserScreenshotTaker();
        argiBrowserScreenshotTaker.setSandbox(sandbox);
        return argiBrowserScreenshotTaker.buildTool();
    }

    public static ToolCallback BrowserCloseTool(Sandbox sandbox) {
        ArgiBrowserCloser argiBrowserCloser = new ArgiBrowserCloser();
        argiBrowserCloser.setSandbox(sandbox);
        return argiBrowserCloser.buildTool();
    }

    public static ToolCallback BrowserHoverTool(Sandbox sandbox) {
        ArgiBrowserHoverer argiBrowserHoverer = new ArgiBrowserHoverer();
        argiBrowserHoverer.setSandbox(sandbox);
        return argiBrowserHoverer.buildTool();
    }

    public static ToolCallback BrowserDragTool(Sandbox sandbox) {
        ArgiBrowserDragger argiBrowserDragger = new ArgiBrowserDragger();
        argiBrowserDragger.setSandbox(sandbox);
        return argiBrowserDragger.buildTool();
    }

    public static ToolCallback BrowserConsoleMessagesTool(Sandbox sandbox) {
        ArgiBrowserConsoleMessagesRetriever argiBrowserConsoleMessagesRetriever = new ArgiBrowserConsoleMessagesRetriever();
        argiBrowserConsoleMessagesRetriever.setSandbox(sandbox);
        return argiBrowserConsoleMessagesRetriever.buildTool();
    }

    public static ToolCallback BrowserFileUploadTool(Sandbox sandbox) {
        ArgiBrowserFileUploader argiBrowserFileUploader = new ArgiBrowserFileUploader();
        argiBrowserFileUploader.setSandbox(sandbox);
        return argiBrowserFileUploader.buildTool();
    }

    public static ToolCallback BrowserHandleDialogTool(Sandbox sandbox) {
        ArgiBrowserDialogHandler argiBrowserDialogHandler = new ArgiBrowserDialogHandler();
        argiBrowserDialogHandler.setSandbox(sandbox);
        return argiBrowserDialogHandler.buildTool();
    }

    public static ToolCallback BrowserNavigateBackTool(Sandbox sandbox) {
        ArgiBrowserBackNavigator argiBrowserBackNavigator = new ArgiBrowserBackNavigator();
        argiBrowserBackNavigator.setSandbox(sandbox);
        return argiBrowserBackNavigator.buildTool();
    }

    public static ToolCallback BrowserNavigateForwardTool(Sandbox sandbox) {
        ArgiBrowserForwardNavigator argiBrowserForwardNavigator = new ArgiBrowserForwardNavigator();
        argiBrowserForwardNavigator.setSandbox(sandbox);
        return argiBrowserForwardNavigator.buildTool();
    }

    public static ToolCallback BrowserNetworkRequestsTool(Sandbox sandbox) {
        ArgiBrowserNetworkRequestsRetriever argiBrowserNetworkRequestsRetriever = new ArgiBrowserNetworkRequestsRetriever();
        argiBrowserNetworkRequestsRetriever.setSandbox(sandbox);
        return argiBrowserNetworkRequestsRetriever.buildTool();
    }

    public static ToolCallback BrowserPdfSaveTool(Sandbox sandbox) {
        ArgiBrowserPdfSaver argiBrowserPdfSaver = new ArgiBrowserPdfSaver();
        argiBrowserPdfSaver.setSandbox(sandbox);
        return argiBrowserPdfSaver.buildTool();
    }

    public static ToolCallback BrowserPressKeyTool(Sandbox sandbox) {
        ArgiBrowserKeyPresser argiBrowserKeyPresser = new ArgiBrowserKeyPresser();
        argiBrowserKeyPresser.setSandbox(sandbox);
        return argiBrowserKeyPresser.buildTool();
    }

    public static ToolCallback BrowserResizeTool(Sandbox sandbox) {
        ArgiBrowserWindowResizer argiBrowserWindowResizer = new ArgiBrowserWindowResizer();
        argiBrowserWindowResizer.setSandbox(sandbox);
        return argiBrowserWindowResizer.buildTool();
    }

    public static ToolCallback BrowserSelectOptionTool(Sandbox sandbox) {
        ArgiBrowserOptionSelector argiBrowserOptionSelector = new ArgiBrowserOptionSelector();
        argiBrowserOptionSelector.setSandbox(sandbox);
        return argiBrowserOptionSelector.buildTool();
    }

    public static ToolCallback BrowserTabCloseTool(Sandbox sandbox) {
        ArgiBrowserTabCloser argiBrowserTabCloser = new ArgiBrowserTabCloser();
        argiBrowserTabCloser.setSandbox(sandbox);
        return argiBrowserTabCloser.buildTool();
    }

    public static ToolCallback BrowserTabListTool(Sandbox sandbox) {
        ArgiBrowserTabLister argiBrowserTabLister = new ArgiBrowserTabLister();
        argiBrowserTabLister.setSandbox(sandbox);
        return argiBrowserTabLister.buildTool();
    }

    public static ToolCallback BrowserTabNewTool(Sandbox sandbox) {
        ArgiBrowserTabCreator argiBrowserTabCreator = new ArgiBrowserTabCreator();
        argiBrowserTabCreator.setSandbox(sandbox);
        return argiBrowserTabCreator.buildTool();
    }

    public static ToolCallback BrowserTabSelectTool(Sandbox sandbox) {
        ArgiBrowserTabSelector argiBrowserTabSelector = new ArgiBrowserTabSelector();
        argiBrowserTabSelector.setSandbox(sandbox);
        return argiBrowserTabSelector.buildTool();
    }

    public static ToolCallback BrowserWaitForTool(Sandbox sandbox) {
        ArgiBrowserWaiter argiBrowserWaiter = new ArgiBrowserWaiter();
        argiBrowserWaiter.setSandbox(sandbox);
        return argiBrowserWaiter.buildTool();
    }

    // Filesystem tools
    public static ToolCallback ReadFileTool(Sandbox sandbox) {
        ArgiFsFileReader argiFsFileReader = new ArgiFsFileReader();
        argiFsFileReader.setSandbox(sandbox);
        return argiFsFileReader.buildTool();
    }

    public static ToolCallback WriteFileTool(Sandbox sandbox) {
        ArgiFsFileWriter argiFsFileWriter = new ArgiFsFileWriter();
        argiFsFileWriter.setSandbox(sandbox);
        return argiFsFileWriter.buildTool();
    }

    public static ToolCallback ListDirectoryTool(Sandbox sandbox) {
        ArgiFsDirectoryLister argiFsDirectoryLister = new ArgiFsDirectoryLister();
        argiFsDirectoryLister.setSandbox(sandbox);
        return argiFsDirectoryLister.buildTool();
    }

    public static ToolCallback CreateDirectoryTool(Sandbox sandbox) {
        ArgiFsDirectoryCreator argiFsDirectoryCreator = new ArgiFsDirectoryCreator();
        argiFsDirectoryCreator.setSandbox(sandbox);
        return argiFsDirectoryCreator.buildTool();
    }

    public static ToolCallback DirectoryTreeTool(Sandbox sandbox) {
        ArgiFsTreeBuilder argiFsTreeBuilder = new ArgiFsTreeBuilder();
        argiFsTreeBuilder.setSandbox(sandbox);
        return argiFsTreeBuilder.buildTool();
    }

    public static ToolCallback EditFileTool(Sandbox sandbox) {
        ArgiFsFileEditor argiFsFileEditor = new ArgiFsFileEditor();
        argiFsFileEditor.setSandbox(sandbox);
        return argiFsFileEditor.buildTool();
    }

    public static ToolCallback GetFileInfoTool(Sandbox sandbox) {
        ArgiFsFileInfoRetriever argiFsFileInfoRetriever = new ArgiFsFileInfoRetriever();
        argiFsFileInfoRetriever.setSandbox(sandbox);
        return argiFsFileInfoRetriever.buildTool();
    }

    public static ToolCallback ListAllowedDirectoriesTool(Sandbox sandbox) {
        ArgiFsAllowedDirectoriesLister argiFsAllowedDirectoriesLister = new ArgiFsAllowedDirectoriesLister();
        argiFsAllowedDirectoriesLister.setSandbox(sandbox);
        return argiFsAllowedDirectoriesLister.buildTool();
    }

    public static ToolCallback MoveFileTool(Sandbox sandbox) {
        ArgiFsFileMover argiFsFileMover = new ArgiFsFileMover();
        argiFsFileMover.setSandbox(sandbox);
        return argiFsFileMover.buildTool();
    }

    public static ToolCallback ReadMultipleFilesTool(Sandbox sandbox) {
        ArgiFsMultiFileReader argiFsMultiFileReader = new ArgiFsMultiFileReader();
        argiFsMultiFileReader.setSandbox(sandbox);
        return argiFsMultiFileReader.buildTool();
    }

    public static ToolCallback SearchFilesTool(Sandbox sandbox) {
        ArgiFsFileSearcher argiFsFileSearcher = new ArgiFsFileSearcher();
        argiFsFileSearcher.setSandbox(sandbox);
        return argiFsFileSearcher.buildTool();
    }

    public static List<ToolCallback> getMcpTools(String serverConfigs,
                                              String sandboxType,
                                              SandboxService sandboxService) {
        return getMcpTools(serverConfigs, sandboxType, sandboxService, null, null);
    }

    public static List<ToolCallback> getMcpTools(Map<String, Object> serverConfigs,
                                              String sandboxType,
                                              SandboxService sandboxService) {
        return getMcpTools(serverConfigs, sandboxType, sandboxService, null, null);
    }

    public static List<ToolCallback> getMcpTools(String serverConfigs,
                                              String sandboxType,
                                              SandboxService sandboxService,
                                              Set<String> whitelist,
                                              Set<String> blacklist) {
        McpConfigConverter converter = McpConfigConverter.builder()
                .serverConfigs(serverConfigs)
                .sandboxType(sandboxType)
                .sandboxService(sandboxService)
                .whitelist(whitelist)
                .blacklist(blacklist)
                .build();

        return buildMcpAgentTools(converter);
    }

    public static List<ToolCallback> getMcpTools(Map<String, Object> serverConfigs,
                                              String sandboxType,
                                              SandboxService sandboxService,
                                              Set<String> whitelist,
                                              Set<String> blacklist) {
        McpConfigConverter converter = McpConfigConverter.builder()
                .serverConfigs(serverConfigs)
                .sandboxType(sandboxType)
                .sandboxService(sandboxService)
                .whitelist(whitelist)
                .blacklist(blacklist)
                .build();

        return buildMcpAgentTools(converter);
    }

    public static List<ToolCallback> getMcpTools(String serverConfigs,
                                              SandboxService sandboxService) {
        return getMcpTools(serverConfigs, null, sandboxService, null, null);
    }

    public static List<ToolCallback> getMcpTools(Map<String, Object> serverConfigs,
                                              SandboxService sandboxService) {
        return getMcpTools(serverConfigs, null, sandboxService, null, null);
    }

    public static List<MCPTool> createMcpToolInstances(String serverConfigs,
                                                       String sandboxType,
                                                       SandboxService sandboxService) {
        McpConfigConverter converter = McpConfigConverter.builder()
                .serverConfigs(serverConfigs)
                .sandboxType(sandboxType)
                .sandboxService(sandboxService)
                .build();

        return converter.toBuiltinTools();
    }

    public static List<MCPTool> createMcpToolInstances(Map<String, Object> serverConfigs,
                                                       String sandboxType,
                                                       SandboxService sandboxService) {
        McpConfigConverter converter = McpConfigConverter.builder()
                .serverConfigs(serverConfigs)
                .sandboxType(sandboxType)
                .sandboxService(sandboxService)
                .build();

        return converter.toBuiltinTools();
    }

    private static List<ToolCallback> buildMcpAgentTools(McpConfigConverter converter) {
        try {
            logger.info("Creating MCP tools from server configuration");

            List<MCPTool> mcpTools = converter.toBuiltinTools();
            List<ToolCallback> agentTools = new ArrayList<>(mcpTools.size());
            for (MCPTool mcpTool : mcpTools) {
                agentTools.add(new ArgiMCPTool(mcpTool).buildTool());
            }

            logger.info("Created {} MCP tools", agentTools.size());
            return agentTools;
        } catch (Exception e) {
            logger.error("Failed to create MCP tools: {}", e.getMessage());
            throw new RuntimeException("Failed to create MCP tools", e);
        }
    }
}
