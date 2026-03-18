---
name: config-impact
description: Analyze the impact of changing a Hudi config. Traces all code paths that read the config to understand what will change. Use before making config changes in production.
user-invocable: true
allowed-tools: Read, Grep, Glob, Agent
argument-hint: [config key e.g. "hoodie.metadata.enable", "hoodie.index.type"]
---

# Hudi Config Impact Analysis

Config: **$ARGUMENTS**

## Instructions

### Step 1: Find the config definition
Search for `ConfigProperty.key("$ARGUMENTS")` to find the definition. Note:
- Default value
- Type
- Whether it has `withAlternatives()` (old key names)
- Whether it has `withInferFunction()` (computed from other configs)
- Whether it has `withValidValues()` (restricted values)

### Step 2: Find all read sites
Search for every place this config is read. Common patterns:
- `getStringOrDefault(CONFIG_PROPERTY)`
- `getBooleanOrDefault(CONFIG_PROPERTY)`
- `getIntOrDefault(CONFIG_PROPERTY)`
- `config.get*()` with the config property
- Direct `props.getProperty("hoodie.xxx")` calls

### Step 3: For each read site, document:
1. **Where**: file:line reference
2. **What it controls**: the decision or behavior gated by this config
3. **Impact of change**: what happens if the value changes from default
4. **Interactions**: other configs read in the same code path

### Step 4: Check validation rules
Look in `HoodieWriteConfig.Builder.validate()` for any validation that references this config.

### Step 5: Check for side effects
- Does changing this config require metadata table rebuild?
- Does it affect existing file format or timeline?
- Is it safe to change on an existing table or only for new tables?
- Does it require a table upgrade?

### Output:
1. **Config definition** - key, type, default, alternatives
2. **Impact map** - every code path affected, grouped by subsystem (write, read, compaction, etc.)
3. **Risk assessment** - safe/caution/dangerous to change on a running table
4. **Rollback plan** - how to revert if the change causes issues
5. **Testing recommendation** - how to validate the change before production
