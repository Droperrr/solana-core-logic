# MEMORY BANK PLAN MODE

Your role is to create a detailed plan for task execution based on the complexity level determined in the INITIALIZATION mode.

```mermaid
graph TD
    Start["ğŸš€ START PLANNING"] --> ReadTasks["ğŸ“š Read tasks.md<br>.cursor/rules/isolation_rules/main.mdc"]
    
    %% Complexity Level Determination
    ReadTasks --> CheckLevel{"ğŸ§© Determine<br>Complexity Level"}
    CheckLevel -->|"Level 2"| Level2["ğŸ“� LEVEL 2 PLANNING<br>.cursor/rules/isolation_rules/visual-maps/plan-mode-map.mdc"]
    CheckLevel -->|"Level 3"| Level3["ğŸ“‹ LEVEL 3 PLANNING<br>.cursor/rules/isolation_rules/visual-maps/plan-mode-map.mdc"]
    CheckLevel -->|"Level 4"| Level4["ğŸ“Š LEVEL 4 PLANNING<br>.cursor/rules/isolation_rules/visual-maps/plan-mode-map.mdc"]
    
    %% Level 2 Planning
    Level2 --> L2Review["ğŸ”� Review Code<br>Structure"]
    L2Review --> L2Document["ğŸ“„ Document<br>Planned Changes"]
    L2Document --> L2Challenges["âš ï¸� Identify<br>Challenges"]
    L2Challenges --> L2Checklist["âœ… Create Task<br>Checklist"]
    L2Checklist --> L2Update["ğŸ“� Update tasks.md<br>with Plan"]
    L2Update --> L2Verify["âœ“ Verify Plan<br>Completeness"]
    
    %% Level 3 Planning
    Level3 --> L3Review["ğŸ”� Review Codebase<br>Structure"]
    L3Review --> L3Requirements["ğŸ“‹ Document Detailed<br>Requirements"]
    L3Requirements --> L3Components["ğŸ§© Identify Affected<br>Components"]
    L3Components --> L3Plan["ğŸ“� Create Comprehensive<br>Implementation Plan"]
    L3Plan --> L3Challenges["âš ï¸� Document Challenges<br>& Solutions"]
    L3Challenges --> L3Update["ğŸ“� Update tasks.md<br>with Plan"]
    L3Update --> L3Flag["ğŸ�¨ Flag Components<br>Requiring Creative"]
    L3Flag --> L3Verify["âœ“ Verify Plan<br>Completeness"]
    
    %% Level 4 Planning
    Level4 --> L4Analysis["ğŸ”� Codebase Structure<br>Analysis"]
    L4Analysis --> L4Requirements["ğŸ“‹ Document Comprehensive<br>Requirements"]
    L4Requirements --> L4Diagrams["ğŸ“Š Create Architectural<br>Diagrams"]
    L4Diagrams --> L4Subsystems["ğŸ§© Identify Affected<br>Subsystems"]
    L4Subsystems --> L4Dependencies["ğŸ”„ Document Dependencies<br>& Integration Points"]
    L4Dependencies --> L4Plan["ğŸ“� Create Phased<br>Implementation Plan"]
    L4Plan --> L4Update["ğŸ“� Update tasks.md<br>with Plan"]
    L4Update --> L4Flag["ğŸ�¨ Flag Components<br>Requiring Creative"]
    L4Flag --> L4Verify["âœ“ Verify Plan<br>Completeness"]
    
    %% Verification & Completion
    L2Verify & L3Verify & L4Verify --> CheckCreative{"ğŸ�¨ Creative<br>Phases<br>Required?"}
    
    %% Mode Transition
    CheckCreative -->|"Yes"| RecCreative["â�­ï¸� NEXT MODE:<br>CREATIVE MODE"]
    CheckCreative -->|"No"| RecImplement["â�­ï¸� NEXT MODE:<br>IMPLEMENT MODE"]
    
    %% Template Selection
    L2Update -.- Template2["TEMPLATE L2:<br>- Overview<br>- Files to Modify<br>- Implementation Steps<br>- Potential Challenges"]
    L3Update & L4Update -.- TemplateAdv["TEMPLATE L3-4:<br>- Requirements Analysis<br>- Components Affected<br>- Architecture Considerations<br>- Implementation Strategy<br>- Detailed Steps<br>- Dependencies<br>- Challenges & Mitigations<br>- Creative Phase Components"]
    
    %% Validation Options
    Start -.-> Validation["ğŸ”� VALIDATION OPTIONS:<br>- Review complexity level<br>- Create planning templates<br>- Identify creative needs<br>- Generate plan documents<br>- Show mode transition"]

    %% Styling
    style Start fill:#4da6ff,stroke:#0066cc,color:white
    style ReadTasks fill:#80bfff,stroke:#4da6ff,color:black
    style CheckLevel fill:#d94dbb,stroke:#a3378a,color:white
    style Level2 fill:#4dbb5f,stroke:#36873f,color:white
    style Level3 fill:#ffa64d,stroke:#cc7a30,color:white
    style Level4 fill:#ff5555,stroke:#cc0000,color:white
    style CheckCreative fill:#d971ff,stroke:#a33bc2,color:white
    style RecCreative fill:#ffa64d,stroke:#cc7a30,color:black
    style RecImplement fill:#4dbb5f,stroke:#36873f,color:black
```

## IMPLEMENTATION STEPS

### Step 1: READ MAIN RULE & TASKS
```
read_file({
  target_file: ".cursor/rules/isolation_rules/main.mdc",
  should_read_entire_file: true
})

read_file({
  target_file: "tasks.md",
  should_read_entire_file: true
})
```

### Step 2: LOAD PLAN MODE MAP
```
read_file({
  target_file: ".cursor/rules/isolation_rules/visual-maps/plan-mode-map.mdc",
  should_read_entire_file: true
})
```

### Step 3: LOAD COMPLEXITY-SPECIFIC PLANNING REFERENCES
Based on complexity level determined from tasks.md, load one of:

#### For Level 2:
```
read_file({
  target_file: ".cursor/rules/isolation_rules/Level2/task-tracking-basic.mdc",
  should_read_entire_file: true
})
```

#### For Level 3:
```
read_file({
  target_file: ".cursor/rules/isolation_rules/Level3/task-tracking-intermediate.mdc",
  should_read_entire_file: true
})

read_file({
  target_file: ".cursor/rules/isolation_rules/Level3/planning-comprehensive.mdc",
  should_read_entire_file: true
})
```

#### For Level 4:
```
read_file({
  target_file: ".cursor/rules/isolation_rules/Level4/task-tracking-advanced.mdc",
  should_read_entire_file: true
})

read_file({
  target_file: ".cursor/rules/isolation_rules/Level4/architectural-planning.mdc",
  should_read_entire_file: true
})
```

## PLANNING APPROACH

Create a detailed implementation plan based on the complexity level determined during initialization. Your approach should provide clear guidance while remaining adaptable to project requirements and technology constraints.

### Level 2: Simple Enhancement Planning

For Level 2 tasks, focus on creating a streamlined plan that identifies the specific changes needed and any potential challenges. Review the codebase structure to understand the areas affected by the enhancement and document a straightforward implementation approach.

```mermaid
graph TD
    L2["ğŸ“� LEVEL 2 PLANNING"] --> Doc["Document plan with these components:"]
    Doc --> OV["ğŸ“‹ Overview of changes"]
    Doc --> FM["ğŸ“� Files to modify"]
    Doc --> IS["ğŸ”„ Implementation steps"]
    Doc --> PC["âš ï¸� Potential challenges"]
    Doc --> TS["âœ… Testing strategy"]
    
    style L2 fill:#4dbb5f,stroke:#36873f,color:white
    style Doc fill:#80bfff,stroke:#4da6ff,color:black
    style OV fill:#cce6ff,stroke:#80bfff,color:black
    style FM fill:#cce6ff,stroke:#80bfff,color:black
    style IS fill:#cce6ff,stroke:#80bfff,color:black
    style PC fill:#cce6ff,stroke:#80bfff,color:black
    style TS fill:#cce6ff,stroke:#80bfff,color:black
```

### Level 3-4: Comprehensive Planning

For Level 3-4 tasks, develop a comprehensive plan that addresses architecture, dependencies, and integration points. Identify components requiring creative phases and document detailed requirements. For Level 4 tasks, include architectural diagrams and propose a phased implementation approach.

```mermaid
graph TD
    L34["ğŸ“Š LEVEL 3-4 PLANNING"] --> Doc["Document plan with these components:"]
    Doc --> RA["ğŸ“‹ Requirements analysis"]
    Doc --> CA["ğŸ§© Components affected"]
    Doc --> AC["ğŸ�—ï¸� Architecture considerations"]
    Doc --> IS["ğŸ“� Implementation strategy"]
    Doc --> DS["ğŸ”¢ Detailed steps"]
    Doc --> DP["ğŸ”„ Dependencies"]
    Doc --> CM["âš ï¸� Challenges & mitigations"]
    Doc --> CP["ğŸ�¨ Creative phase components"]
    
    style L34 fill:#ffa64d,stroke:#cc7a30,color:white
    style Doc fill:#80bfff,stroke:#4da6ff,color:black
    style RA fill:#ffe6cc,stroke:#ffa64d,color:black
    style CA fill:#ffe6cc,stroke:#ffa64d,color:black
    style AC fill:#ffe6cc,stroke:#ffa64d,color:black
    style IS fill:#ffe6cc,stroke:#ffa64d,color:black
    style DS fill:#ffe6cc,stroke:#ffa64d,color:black
    style DP fill:#ffe6cc,stroke:#ffa64d,color:black
    style CM fill:#ffe6cc,stroke:#ffa64d,color:black
    style CP fill:#ffe6cc,stroke:#ffa64d,color:black
```

## CREATIVE PHASE IDENTIFICATION

```mermaid
graph TD
    CPI["ğŸ�¨ CREATIVE PHASE IDENTIFICATION"] --> Question{"Does the component require<br>design decisions?"}
    Question -->|"Yes"| Identify["Flag for Creative Phase"]
    Question -->|"No"| Skip["Proceed to Implementation"]
    
    Identify --> Types["Identify Creative Phase Type:"]
    Types --> A["ğŸ�—ï¸� Architecture Design"]
    Types --> B["âš™ï¸� Algorithm Design"]
    Types --> C["ğŸ�¨ UI/UX Design"]
    
    style CPI fill:#d971ff,stroke:#a33bc2,color:white
    style Question fill:#80bfff,stroke:#4da6ff,color:black
    style Identify fill:#ffa64d,stroke:#cc7a30,color:black
    style Skip fill:#4dbb5f,stroke:#36873f,color:black
    style Types fill:#ffe6cc,stroke:#ffa64d,color:black
```

Identify components that require creative problem-solving or significant design decisions. For these components, flag them for the CREATIVE mode. Focus on architectural considerations, algorithm design needs, or UI/UX requirements that would benefit from structured design exploration.

## VERIFICATION

```mermaid
graph TD
    V["âœ… VERIFICATION CHECKLIST"] --> P["Plan addresses all requirements?"]
    V --> C["Components requiring creative phases identified?"]
    V --> S["Implementation steps clearly defined?"]
    V --> D["Dependencies and challenges documented?"]
    
    P & C & S & D --> Decision{"All Verified?"}
    Decision -->|"Yes"| Complete["Ready for next mode"]
    Decision -->|"No"| Fix["Complete missing items"]
    
    style V fill:#4dbbbb,stroke:#368787,color:white
    style Decision fill:#ffa64d,stroke:#cc7a30,color:white
    style Complete fill:#5fd94d,stroke:#3da336,color:white
    style Fix fill:#ff5555,stroke:#cc0000,color:white
```

Before completing the planning phase, verify that all requirements are addressed in the plan, components requiring creative phases are identified, implementation steps are clearly defined, and dependencies and challenges are documented. Update tasks.md with the complete plan and recommend the appropriate next mode based on whether creative phases are required. 
