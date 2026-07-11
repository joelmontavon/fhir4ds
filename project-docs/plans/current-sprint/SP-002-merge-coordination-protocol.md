# SP-002 Merge Coordination Protocol

**Sprint**: SP-002 - FHIRPath Foundation Critical Completion
**Purpose**: Prevent merge conflicts through coordinated integration
**Created**: 26-01-2025

---

## Current Status

### ✅ **Completed Merges**
1. **SP-002-001**: Test suite integration - **MERGED** to complete-rewrite
2. **SP-002-003**: Advanced literal support - **MERGED** to complete-rewrite

### 🔄 **Pending Integration**
3. **SP-002-002**: Core function library - **IN DEVELOPMENT**
4. **SP-002-004**: Architecture compliance fixes - **PENDING**
5. **SP-002-005**: Performance validation - **PENDING**
6. **SP-002-006**: Documentation updates - **PENDING**

---

## Integration Order Protocol

### **Phase 1: Foundation Complete ✅**
- **SP-002-001** (Test Infrastructure) ✅ Merged
- **SP-002-003** (Advanced Literals) ✅ Merged

**Status**: Foundation established, no more fundamental parser conflicts expected.

### **Phase 2: Core Functionality (Current)**
- **SP-002-002** (Core Function Library) - **ACTIVE DEVELOPMENT**

**Requirements before merge**:
- [ ] Rebase against current complete-rewrite (includes SP-002-001 + SP-002-003)
- [ ] All function parsing tests pass
- [ ] No regression in literal parsing
- [ ] Visitor pattern updated for all new function nodes
- [ ] Integration test with existing compliance framework

### **Phase 3: Architecture & Performance**
- **SP-002-004** (Architecture Compliance) - **WAIT** for SP-002-002 completion
- **SP-002-005** (Performance Validation) - **PARALLEL** with SP-002-004

**Coordination**: These can run parallel as they don't modify core parser files.

### **Phase 4: Documentation**
- **SP-002-006** (Documentation) - **FINAL** after all technical work

---

## Merge Conflict Prevention Rules

### **Pre-Merge Checklist**

#### For All Feature Branches:
- [ ] **Rebase required**: `git rebase origin/complete-rewrite`
- [ ] **Test suite passes**: All parser and compliance tests green
- [ ] **No file conflicts**: Clean rebase with no manual conflict resolution needed
- [ ] **Review approval**: Senior Architect approval obtained

#### For Parser-Heavy Branches (SP-002-002):
- [ ] **Function integration tested**: All new functions parse correctly
- [ ] **Visitor pattern complete**: All AST visitors updated
- [ ] **Error handling preserved**: No regression in parser error messages
- [ ] **Source location tracking**: Maintained for all new nodes

### **Communication Protocol**

#### Before Starting Parser Work:
1. **Announce intent**: "Starting work on SP-002-XXX, will modify parser.py"
2. **Check coordination**: Ensure no other parser work active
3. **Rebase immediately**: Start from latest complete-rewrite

#### Before Creating PR:
1. **Final rebase**: `git rebase origin/complete-rewrite`
2. **Force push**: `git push --force-with-lease origin feature/branch`
3. **Test verification**: Run full test suite locally
4. **Announce readiness**: "SP-002-XXX ready for review and merge"

#### During Review:
1. **No new commits**: Avoid pushing new commits during review
2. **Address feedback**: Use `git commit --fixup` for small changes
3. **Squash commits**: Clean up history before final merge

---

## Conflict Resolution Strategy

### **If Conflicts Occur Despite Protocol**

#### Step 1: Assess Conflict Scope
```bash
git status                           # See conflicted files
git diff --name-only --diff-filter=U # List unmerged files
```

#### Step 2: Parser Conflict Resolution
```bash
# For parser.py conflicts:
# 1. Identify which branch has more complete functionality
# 2. Take that version as base
# 3. Manually merge missing features from other branch
# 4. Test thoroughly

# Example approach:
git checkout feature/more-complete-branch -- fhir4ds/parser/parser.py
git add fhir4ds/parser/parser.py
# Manually add missing pieces from other branch
# Test all functionality
git commit -m "resolve: merge parser functionality from both branches"
```

#### Step 3: AST Node Conflicts
```bash
# For nodes.py conflicts:
# 1. Merge new node definitions from both branches
# 2. Ensure visitor pattern methods exist for all nodes
# 3. Verify import statements include all new classes
# 4. Test AST construction for all node types

# Key areas to check:
# - All new Literal classes included
# - All new Expression classes included
# - Visitor method signatures match node classes
# - Import statements complete
```

#### Step 4: Verification Testing
```bash
# Required tests after conflict resolution:
python -c "from fhir4ds.parser.core import parse; parse('@2024-01-01')"    # Literals
python -c "from fhir4ds.parser.core import parse; parse('Patient.name')"   # Basic parsing
python -c "from fhir4ds.parser.core import parse; parse('{1, 2, 3}')"      # Collections
python -c "from fhir4ds.parser.core import parse; parse('count()')"        # Functions (when available)

# Run compliance suite:
# (when SP-002-001 compliance framework available)
```

---

## Lessons Learned

### **What Worked Well**
1. **SP-002-001 first**: Establishing test infrastructure prevented issues
2. **Automatic merges**: When branches don't overlap, Git handles merges cleanly
3. **Documentation updates**: Clear protocols reduce uncertainty

### **What to Improve**
1. **Earlier coordination**: Announce parser work before starting, not after
2. **Daily rebases**: Keep feature branches current during development
3. **Smaller increments**: Consider breaking SP-002-002 into smaller parser changes

### **Future Prevention**
1. **Branch naming**: Include coordination info: `feature/sp-002-002-parser-functions-ACTIVE`
2. **Daily standups**: Include merge coordination status
3. **Merge windows**: Designated times for integration to avoid overlap

---

## Next Steps

### **Immediate Actions**
1. **Monitor SP-002-002**: Ensure clean rebase before PR creation
2. **Plan SP-002-004**: Coordinate with SP-002-002 completion
3. **Update team**: Communicate new merge coordination protocols

### **Process Improvements**
1. **Automated checks**: Consider pre-commit hooks for rebase verification
2. **Conflict detection**: Early warning system for overlapping parser work
3. **Integration testing**: Automated verification of merger integration

---

**This protocol ensures SP-002 sprint completion without recurring merge conflicts through systematic coordination and clear integration ordering.**