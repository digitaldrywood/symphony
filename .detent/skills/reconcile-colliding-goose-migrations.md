---
name: reconcile-colliding-goose-migrations
description: Repair a Goose migration-number collision while preserving databases initialized from either independently valid branch.
when_to_use: Parallel migration additions pass separately, collide after integration, and either branch may already have persisted its shared version number.
---

Compare migration introduction commits, the CI checkout commit and parents, the
final merge parent, and the merge readiness code. A green PR can have tested a
synthetic merge against an older base. Do not infer which actor or program merged
from a shared GitHub credential alone. Check whether application gates rely on
server refusal and whether branch protection applies to that credential.

Keep the earlier landed version stable. Goose tracks version numbers, not file
names: renaming the later file does not make the earlier migration execute on a
database that already recorded that number for the other branch. Enumerate the
actual schema variants before choosing a repair.

For compatible additive tables, a later transactional, forward-only migration
can create only the missing objects while preserving existing rows. Conditional
creation is appropriate only when the supported historical object definitions
are equivalent. Other schema differences need explicit reconciliation; do not
silently stamp versions or drop persisted tables.

Freeze the historical migration as a test fixture. Seed meaningful data before
upgrading each branch variant, then check schema versions, row contents, foreign
keys, integrity and restart behavior. Test fresh startup and the valid shared
prior schema too. Parse Goose fixture headers independently of LF/CRLF line
endings and exercise both: Windows checkout conversion can otherwise leave a
second Up directive in combined historical fixtures. Document backup and
recovery through the supported binary.

Reject numeric duplicates within each schema namespace before expensive gates.
Use a real conflict-free Git merge regression with two independently valid
branches. Enforce current-base integration before accepting stale green checks;
keep current-head CI and server-side protection. Coordinate dependent branches to
consume one repair and allocate subsequent versions after it.
