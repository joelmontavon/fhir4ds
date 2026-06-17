# Git Workflow

**Document Version**: 1.0
**Date**: 2025-01-21
**Status**: Development Process

---

## Overview

This document defines the Git workflow and version control practices for FHIR4DS development. The workflow is designed to support collaborative development while maintaining code quality and architectural consistency.

## Branching Strategy

### Main Branches

#### `main` Branch
- **Purpose**: Production-ready code
- **Protection**: No direct commits allowed
- **Merge Requirements**: All merges require pull request review and approval
- **Quality Gates**: All tests must pass, compliance maintained
- **Release Source**: All releases are tagged from main branch

#### `develop` Branch (if used)
- **Purpose**: Integration branch for feature development
- **Protection**: No direct commits allowed
- **Merge Source**: Features merge to develop before main
- **Testing**: Continuous integration runs on all commits

### Feature Branches

#### Naming Convention
```
feature/PEP-XXX-brief-description
feature/issue-XXX-brief-description
feature/brief-functional-description
```

#### Examples
```
feature/PEP-001-fhirpath-unification
feature/issue-123-boundary-function-fix
feature/add-postgresql-dialect-support
```

#### Lifecycle
1. **Create** from main (or develop)
2. **Develop** with regular commits
3. **Test** thoroughly in both database environments
4. **Create Pull Request** when ready for review
5. **Address Review Feedback** as needed
6. **Merge** after approval and quality gates pass
7. **Delete** feature branch after successful merge

### Bug Fix Branches

#### Naming Convention
```
fix/issue-XXX-brief-description
fix/critical-bug-brief-description
hotfix/urgent-production-fix
```

#### Examples
```
fix/issue-456-null-pointer-exception
fix/critical-performance-regression
hotfix/sql-injection-vulnerability
```

### Process Improvement Branches

#### Naming Convention
```
process/brief-description
docs/documentation-update
chore/maintenance-task
```

#### Examples
```
process/update-coding-standards
docs/update-architecture-diagrams
chore/upgrade-dependencies
```

---

## Commit Standards

### Conventional Commits Format

All commits must follow the [Conventional Commits](https://www.conventionalcommits.org/) specification:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Commit Types

#### Primary Types
- **feat**: New feature or enhancement
- **fix**: Bug fix
- **docs**: Documentation changes
- **style**: Code style changes (formatting, no logic changes)
- **refactor**: Code refactoring without adding features or fixing bugs
- **perf**: Performance improvements
- **test**: Adding or updating tests
- **chore**: Maintenance tasks, dependency updates

#### FHIR4DS-Specific Types
- **compliance**: Changes to improve specification compliance
- **dialect**: Database dialect-specific changes
- **arch**: Architectural changes or improvements

### Commit Scope Examples
- **fhirpath**: FHIRPath-related changes
- **cql**: CQL-related changes
- **sql-on-fhir**: SQL-on-FHIR changes
- **dialects**: Database dialect changes
- **tests**: Test-related changes
- **docs**: Documentation changes
- **config**: Configuration changes

### Commit Message Examples

#### Good Commit Messages
```
feat(fhirpath): implement lowBoundary and highBoundary functions
fix(dialects): correct JSON path extraction for nested arrays
compliance(sql-on-fhir): achieve 100% test suite compliance
arch(engine): add unified FHIRPath execution foundation
dialect(duckdb): add json_extract syntax for DuckDB dialect
docs(architecture): update goals with unified FHIRPath approach
```

#### Poor Commit Messages (Avoid These)
```
fix bug                          # Too vague
updated files                    # No useful information
WIP                             # Work in progress should not be in main
fixed the thing that was broken  # Non-descriptive
asdf                           # Meaningless
feat: Claude implemented new feature  # Don't mention people/agents
```

### Atomic Commits

Each commit should represent **one logical change**:

#### Good Atomic Commits
- Add a single function with its tests
- Fix one specific bug
- Update documentation for one feature
- Refactor one component

#### Bad Non-Atomic Commits
- Mix bug fixes with new features
- Update multiple unrelated components
- Combine code changes with documentation updates
- Include formatting changes with logic changes

---

## Pull Request Process

### Creating Pull Requests

#### PR Title Format
Follow the same format as commit messages:
```
<type>(scope): brief description of changes
```

#### PR Description Template
```markdown
## Summary
Brief description of what this PR accomplishes.

## Changes Made
- [ ] List of specific changes
- [ ] Each change as a checkbox
- [ ] Include testing performed

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests passing
- [ ] DuckDB environment tested
- [ ] PostgreSQL environment tested
- [ ] Official test suites passing

## Compliance Impact
- [ ] FHIRPath compliance: X% → Y%
- [ ] SQL-on-FHIR compliance maintained
- [ ] CQL compliance: X% → Y%
- [ ] No regression in test results

## Documentation
- [ ] Code comments updated
- [ ] API documentation updated
- [ ] Architecture documentation updated (if applicable)
- [ ] User documentation updated (if applicable)

## Related Issues
- Closes #XXX
- Implements PEP-XXX
- Related to #YYY

## Review Checklist
- [ ] Code follows established patterns
- [ ] No hardcoded values introduced
- [ ] Error handling is comprehensive
- [ ] Security considerations addressed
- [ ] Performance impact assessed
```

### Review Process

#### Reviewer Responsibilities (Senior Solution Architect/Engineer)
1. **Architecture Review**: Ensure changes align with system architecture
2. **Code Quality**: Verify code follows established standards
3. **Testing**: Confirm adequate test coverage and validation
4. **Compliance**: Verify specification compliance is maintained or improved
5. **Security**: Review for security implications
6. **Performance**: Assess performance impact
7. **Documentation**: Ensure documentation is complete and accurate

#### Review Timeline
- **Standard PRs**: Review within 24 hours during business days
- **Urgent/Hotfix PRs**: Review within 4 hours
- **Large PRs**: May require additional review time, coordinate in advance

#### Review Feedback Categories

**Must Fix (Blocking)**
- Security vulnerabilities
- Broken functionality
- Architecture violations
- Test failures
- Compliance regressions

**Should Fix (Non-blocking but important)**
- Performance concerns
- Code style inconsistencies
- Missing documentation
- Incomplete test coverage

**Suggestions (Optional improvements)**
- Code optimization opportunities
- Alternative implementation approaches
- Future enhancement considerations

### Approval Process

#### Single Approver Model
- **Senior Solution Architect/Engineer** has final approval authority
- **Junior Developer** creates PRs and addresses feedback
- All PRs require explicit approval before merge

#### Quality Gates
Before approval, all PRs must pass:
- [ ] All automated tests (unit, integration, compliance)
- [ ] Code quality checks (linting, formatting)
- [ ] Security scans (if applicable)
- [ ] Manual review by Senior Solution Architect/Engineer
- [ ] Documentation completeness check

---

## Merge Strategies

### Squash and Merge (Preferred)
- **Use Case**: Feature branches with multiple development commits
- **Benefit**: Clean main branch history with atomic changes
- **Requirement**: Final commit message must follow conventional commit format

### Merge Commit
- **Use Case**: Significant features that benefit from preserving development history
- **Requirement**: All individual commits must follow conventional commit standards
- **Approval**: Senior Solution Architect/Engineer must explicitly approve merge commit strategy

### Rebase and Merge (Restricted)
- **Use Case**: Only for simple, single-commit PRs
- **Requirement**: Commit already follows conventional commit format
- **Caution**: Can cause issues with shared branches

---

## Release Management

### Version Numbering
Follow [Semantic Versioning](https://semver.org/):
- **MAJOR.MINOR.PATCH** (e.g., 1.2.3)
- **MAJOR**: Breaking changes or major architectural updates
- **MINOR**: New features, specification compliance improvements
- **PATCH**: Bug fixes, performance improvements

### Release Process

#### Pre-Release Checklist
1. **Compliance Validation**
   - [ ] All official test suites passing
   - [ ] Compliance metrics meet or exceed targets
   - [ ] No critical test failures

2. **Quality Assurance**
   - [ ] All tests passing in both database environments
   - [ ] Performance benchmarks met
   - [ ] Security scan completed
   - [ ] Documentation complete and current

3. **Release Preparation**
   - [ ] CHANGELOG updated with all changes
   - [ ] Version numbers updated in all relevant files
   - [ ] Migration scripts prepared (if needed)
   - [ ] Release notes drafted

#### Release Workflow
1. **Create Release Branch**: `release/vX.Y.Z`
2. **Final Testing**: Complete validation in release branch
3. **Create Release PR**: Merge release branch to main
4. **Tag Release**: Create annotated tag on main branch
5. **Deploy**: Follow deployment procedures
6. **Post-Release**: Merge main back to develop (if using)

#### Hotfix Process
For urgent production fixes:

1. **Create Hotfix Branch**: `hotfix/vX.Y.Z+1` from main
2. **Implement Fix**: Minimal changes to address issue
3. **Test Thoroughly**: Ensure fix doesn't break anything
4. **Fast-Track Review**: Expedited review process
5. **Emergency Deployment**: Deploy as soon as safe
6. **Backport**: Apply fix to develop branch

---

## Collaboration Guidelines

### Communication

#### Git State Communication Protocol
To avoid confusion about work completion status, all communication about git operations must clearly distinguish between different states:

**Git States and Communication**:
1. **Working Directory Changes**: "I've created/updated files" (not visible to others)
2. **Staged Changes**: "Changes are staged for commit" (not visible to others)
3. **Local Commits**: "Changes committed locally, ready for push" (not visible on GitHub)
4. **Pushed to Remote**: "Changes pushed to GitHub at [URL]" (visible to everyone)
5. **Merged to Main**: "Changes merged to main branch" (task truly complete)

**Required Communication Format**:
- ❌ **Unclear**: "I've completed the task" or "Everything is committed"
- ✅ **Clear**: "Work committed locally, ready for you to push: `git push origin complete-rewrite`"
- ✅ **Clear**: "Changes pushed to GitHub and visible at https://github.com/user/repo/tree/branch/path"

**Work Completion Definition**:
- **Tasks are NOT complete** until merged to main branch
- **Local commits** are preparation work, not completion
- **GitHub visibility** is required for review and collaboration
- **Main branch merge** is the final completion milestone

**Status Update Requirements**:
- Always specify exact git state when reporting progress
- Provide GitHub URLs when work should be visible
- Indicate required actions: "Ready for push", "Ready for review", "Ready for merge"
- Include exact commands when user action needed: `git push origin branch-name`

#### Commit Messages as Communication
- Write commit messages for future developers (including yourself)
- Explain **why** changes were made, not just **what** changed
- Reference relevant issues, PEPs, or specifications

#### PR Discussions
- Be constructive and specific in feedback
- Explain the reasoning behind suggestions
- Ask questions when unclear about implementation decisions
- Acknowledge good practices and improvements

#### Developer-to-User Handoff Protocol
When development work is ready for user review/action:

**Immediate Notification**: "Work committed locally, ready for push"
**Push Suggestion**: "Recommend pushing to GitHub: `git push origin branch-name`"
**Review Request**: "Ready for review when pushed to GitHub"
**Next Steps**: "After review, ready for PR creation and merge"

**GitHub URL Verification**: Always provide specific GitHub URLs where work should be visible after push:
- Branch URL: `https://github.com/user/repo/tree/branch-name`
- File URLs: `https://github.com/user/repo/blob/branch-name/path/to/file.md`
- Diff URLs: `https://github.com/user/repo/compare/main...branch-name`

### Conflict Resolution

#### Merge Conflicts
1. **Fetch Latest**: Always fetch latest changes before creating PRs
2. **Rebase Regularly**: Keep feature branches up to date with main
3. **Resolve Promptly**: Address conflicts as soon as they're identified
4. **Test After Resolution**: Ensure tests still pass after conflict resolution

#### Process Conflicts
- **Technical Disagreements**: Senior Solution Architect/Engineer makes final decision
- **Process Issues**: Discuss in team meetings and document decisions
- **Escalation Path**: Document unresolved issues for future process improvement

---

## Git Configuration

### Required Git Configuration
```bash
# User identification
git config user.name "Your Full Name"
git config user.email "your.email@domain.com"

# Conventional commit message template
git config commit.template ~/.gitmessage.txt

# Default branch
git config init.defaultBranch main

# Push settings
git config push.default simple
git config push.followTags true

# Rebase settings
git config pull.rebase true
git config rebase.autoStash true

# Merge settings
git config merge.ff only
```

### Recommended Commit Message Template
Create `~/.gitmessage.txt`:
```
# <type>(<scope>): <subject>
#
# <body>
#
# <footer>
#
# Type should be one of the following:
# * feat: new feature
# * fix: bug fix
# * docs: documentation changes
# * style: formatting changes
# * refactor: code refactoring
# * perf: performance improvements
# * test: adding tests
# * chore: maintenance
# * compliance: specification compliance
# * dialect: database dialect changes
# * arch: architectural changes
#
# Scope examples: fhirpath, cql, sql-on-fhir, dialects, tests, docs
# Subject line should be <= 50 characters
# Body should explain what and why, not how
# Footer should reference issues: "Closes #123" or "Implements PEP-001"
```

### Git Hooks (Recommended)

#### Pre-commit Hook
```bash
#!/bin/sh
# Run linting and formatting
python -m black --check .
python -m flake8 .
python -m mypy .

# Run quick tests
python -m pytest tests/unit/ --quiet

# Check for secrets or sensitive data
git diff --cached --name-only | xargs grep -l "password\|secret\|key" || true
```

#### Pre-push Hook
```bash
#!/bin/sh
# Run full test suite before pushing
python -m pytest tests/

# Run compliance tests
python tests/run_tests.py --dialect all
```

---

## Troubleshooting

### Common Issues

#### "Branch is X commits behind main"
```bash
# Update feature branch with latest main
git checkout feature/my-branch
git fetch origin
git rebase origin/main
```

#### "Merge conflicts in multiple files"
```bash
# Start interactive rebase to clean up history
git rebase -i origin/main

# Resolve conflicts file by file
git add resolved-file.py
git rebase --continue
```

#### "Commit message doesn't follow conventional format"
```bash
# Amend last commit message
git commit --amend

# Interactive rebase to fix multiple commits
git rebase -i HEAD~3  # Fix last 3 commits
```

#### "Tests failing after merge"
```bash
# Run specific test categories
python -m pytest tests/unit/
python -m pytest tests/integration/ --dialect duckdb
python -m pytest tests/integration/ --dialect postgresql

# Run compliance tests
python tests/run_comprehensive_compliance.py
```

### Recovery Procedures

#### Accidental Commit to Main
```bash
# Never force push to main - instead create revert commit
git revert HEAD
git push origin main
```

#### Lost Work Due to Rebase
```bash
# Find lost commits using reflog
git reflog
git cherry-pick <lost-commit-hash>
```

#### Corrupted Repository
```bash
# Clone fresh copy and apply changes
git clone <repository-url> fhir4ds-fresh
cd fhir4ds-fresh
# Manually apply uncommitted changes
```

---

## Best Practices Summary

### Do's ✅
- **Write descriptive commit messages** following conventional commit format
- **Create atomic commits** representing single logical changes
- **Test thoroughly** in both database environments before creating PRs
- **Keep branches focused** on single features or bug fixes
- **Rebase feature branches** to keep history clean
- **Reference issues and PEPs** in commit messages and PRs
- **Review code promptly** to maintain development velocity

### Don'ts ❌
- **Never commit directly** to main or develop branches
- **Don't mix unrelated changes** in single commits
- **Avoid force pushing** to shared branches
- **Don't ignore test failures** in PRs
- **Never commit secrets** or hardcoded credentials
- **Don't leave TODO comments** without creating issues
- **Avoid massive PRs** that are difficult to review

### Emergency Procedures
- **Production Issues**: Create hotfix branch immediately
- **Security Vulnerabilities**: Follow security incident response plan
- **Data Loss**: Contact Senior Solution Architect/Engineer immediately
- **Repository Corruption**: Clone fresh and restore from backups

---

## Conclusion

This Git workflow balances collaboration efficiency with code quality and architectural consistency. By following these practices, the FHIR4DS development team can maintain a clean, auditable development history while progressing systematically toward 100% specification compliance.

Regular review and refinement of these workflow practices ensures they continue to support effective development as the project and team evolve.

---

*This workflow is designed to support the PEP-inspired development process while maintaining the architectural principles and quality standards essential to FHIR4DS success.*