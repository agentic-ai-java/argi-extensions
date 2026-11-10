# Nacos Prompt auto-configuration

`argi.nacos.prompt.template.enabled` controls the prompt template factory and its
builder configuration. It defaults to `false`. Set it to `true` to enable these
beans; application-provided beans still take precedence.

`spring.nacos.config.enabled` independently controls the Nacos Config integration.
Evaluating the prompt condition does not change this property or any JVM system
property. Disabling Prompt does not disable Nacos Config used elsewhere in the
application.

## Migration

Previously, leaving Prompt disabled implicitly set the JVM system property
`spring.nacos.config.enabled=false`, including when the application explicitly
enabled Nacos Config. This also affected other application contexts in the JVM.

Applications that relied on that side effect and do not need Nacos Config should
now explicitly configure `spring.nacos.config.enabled=false`. Applications that
use Nacos Config should keep their own connection configuration; disabling Prompt
must not hide Nacos Config configuration or connection failures.

The regression tests cover the Prompt auto-configuration offline. They do not
verify connections to a live Nacos server.
