# Component Landscape and Optimization Potentials

## Purpose
This document focuses on the platform as an ecosystem of interacting component families. It complements the architecture documents with a more explicit product and systems view.

## Current Component Landscape

### Core Platform Components
- DB and persistence layer
- pipeline store
- loader and render path
- engine and execution layers
- validator and advice layer
- MCP handlers as product interface
- brick registry
- helper registry
- help and changelog surfaces

### Observed Structural Imbalance
The ecosystem is moving toward brick-first, but the current platform still shows heavy usage of:
- helper-based behavior
- raw `mcp.call` steps
- pipeline-local logic
- runtime-semantic duplication across layers

## System-Level Optimization Potentials

### 1. Semantic Consolidation
Highest-value platform optimization.

Problem:
- multiple layers reconstruct step meaning

Optimization:
- unify around one effective step model

Impact:
- lower bug surface
- better validator/runtime parity
- easier debugging

### 2. Registry-Centric Product Surface
Problem:
- registry knowledge, runtime behavior, help, and MCP documentation are not always equally strong

Optimization:
- drive more product surfaces directly from registry and DB metadata

Impact:
- less drift
- stronger LLM guidance

### 3. Helper Reduction
Problem:
- helpers are still a major reuse mechanism

Optimization:
- cluster and migrate helper families into stable bricks

Impact:
- more reuse
- less hidden logic
- easier onboarding

### 4. MCP Surface Strengthening
Problem:
- too many operations still require mental reconstruction

Optimization:
- add context, graph, similarity, and repair MCP surfaces

Impact:
- more self-explanatory system behavior

### 5. Observability as Product Capability
Problem:
- the platform can still make users reconstruct execution meaning manually

Optimization:
- improve failure phase visibility, effective-step inspection, and prior-case-aware diagnosis

Impact:
- shorter repair loops

## Product Feature Opportunities

### Feature Family: Reuse Intelligence
- similar pipeline search
- similar intent search
- brick candidate generation
- repeated pattern clustering

### Feature Family: Governance Intelligence
- missing metadata inventory
- lifecycle inconsistencies
- helper-without-justification reports
- drift dashboards

### Feature Family: Composition Intelligence
- suggest best-fit bricks for an intent
- show compatible downstream bricks by output contract
- recommend replacement of raw `mcp.call` or helper paths

### Feature Family: Onboarding Intelligence
- component context views
- dependency maps
- historical decision trails
- why-does-this-exist answers

### Feature Family: Operational Safety
- strict authoring modes
- mandatory reuse checks
- mandatory workaround annotation
- similarity-aware run diagnosis

## Platform Smells That Should Become Measured Signals
- helper density per project
- raw `mcp.call` density per project
- repeated SQL pattern density
- repeated subpipeline pattern density
- missing metadata density
- workaround density
- legacy type density
- drift density

## Suggested Strategic Metrics
- percent of active pipelines with full required metadata
- percent of active helpers marked as brick candidates
- percent of repeated patterns covered by reusable bricks
- mean time to identify root cause
- percent of new components created after reuse check
- number of prior-case matches surfaced per incident

## Long-Term Direction
The platform should evolve from “workflow runtime with DB-backed state” into:
- workflow platform
- knowledge system
- architecture guidance engine
- reuse engine

That is the real product opportunity.
