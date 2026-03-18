---
name: hudi-config
description: Find, explain, and recommend Hudi configuration settings. Use when someone asks about configs, tuning, performance settings, or wants to know what a config does, its default, alternatives, and impact.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [config key or topic e.g. "hoodie.compaction.strategy", "compaction tuning", "file sizing"]
---

# Hudi Configuration Helper

The user wants help with Hudi configuration: **$ARGUMENTS**

## Instructions

### If the user provides a specific config key:
1. Search for the `ConfigProperty` definition using Grep: `ConfigProperty.key("$ARGUMENTS")` or partial match
2. Read the full definition including: default value, documentation, sinceVersion, alternatives, validValues, inferFunction
3. Search for where this config is **read** in the codebase (look for `get*()` calls with the config property)
4. Identify what code paths this config affects

Report:
- **Config key**: full dot-separated key
- **Type**: String/Boolean/Integer/Long/etc.
- **Default**: value and why
- **Since**: version introduced
- **Advanced?**: whether marked as advanced
- **Alternatives**: deprecated/old key names
- **What it controls**: which code paths read it (with file:line references)
- **Interactions**: other configs it affects or is affected by (check `withInferFunction` and validation in `HoodieWriteConfig.Builder.validate()`)
- **Recommendation**: when to change from default and to what values

### If the user provides a topic (e.g. "compaction tuning"):
1. Find the relevant config class(es):
   - `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/` for write-side configs
   - `hudi-common/src/main/java/org/apache/hudi/common/config/` for common configs
2. List all configs in that area with their defaults
3. Group them by: essential (most users need), advanced (power users), and rarely-changed
4. For each essential config, explain when and why to change it

### Config validation
Check `HoodieWriteConfig.Builder.validate()` at `hudi-client/hudi-client-common/src/main/java/org/apache/hudi/config/HoodieWriteConfig.java` for cross-config validation rules that apply.

### Always include:
- Spark SQL property syntax: `SET hoodie.xxx.yyy=value;`
- DataFrame option syntax: `.option("hoodie.xxx.yyy", "value")`
- Hudi-defaults.conf syntax for cluster-wide defaults
