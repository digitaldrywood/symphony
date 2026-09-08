---
name: provider-fixture-account-continuity
description: Preserve the selected actor in browser identity-provider fixtures across application-session rotation and revocation.
when_to_use: Invitation, membership, or grant browser evidence unexpectedly returns to a privileged account after provider sign-in.
---

- Model the provider's selected account separately from the application's session.
  A revoked or rotated app cookie must not select a default privileged actor.
- Keep explicit account selection restricted to known synthetic identities in
  the test preview. Continue through the real application's state, code exchange
  and session handling.
- Add table-driven cases for privileged and restricted actors: select the actor,
  invalidate the app session, sign in again and assert the returned identity.
- Verify permissions after invitation acceptance and grant changes. A successful
  page load alone does not prove which actor completed the journey.
- Use a fixture mailbox for provider messages. Access a running service through
  its owned interfaces instead of opening its exclusively owned database.
- Capture both denied access before a grant and allowed access afterward, while
  checking that privileged controls remain absent for the restricted actor.
- Exclude captures made under the wrong identity and regenerate them after the
  fixture correction. Keep all identities and messages synthetic.
