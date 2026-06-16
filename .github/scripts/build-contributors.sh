#!/usr/bin/env bash
# Bake the landing page's contributor list into docs/contributors.json, thanking
# only external contributors — the org's members & owners are excluded.
#
# Org members are the repo collaborators that are NOT outside collaborators
# (affiliation=all minus affiliation=outside). This catches private memberships,
# which an unauthenticated browser call cannot see. Listing collaborators needs
# only `metadata: read`.
#
# Resilient by design: any API failure bakes an empty list so the contributors
# section simply stays hidden and the deploy still ships.
#
# Requires: gh (authenticated via GH_TOKEN) and jq. Reads $GITHUB_REPOSITORY.

repo="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY not set}"

generate() {
  set -euo pipefail
  gh api --paginate "repos/$repo/collaborators?affiliation=all" \
    --jq '.[].login' | LC_ALL=C sort -u > all.txt
  gh api --paginate "repos/$repo/collaborators?affiliation=outside" \
    --jq '.[].login' | LC_ALL=C sort -u > outside.txt
  LC_ALL=C comm -23 all.txt outside.txt > members.txt
  gh api --paginate "repos/$repo/contributors" \
    --jq '.[] | select(.type == "User") | {login, html_url, avatar_url}' \
    | jq -s '.' > contributors.json
  jq --rawfile members members.txt '
    ($members | split("\n") | map(select(length > 0))) as $m
    | map(select((.login) as $l | ($m | index($l)) == null))
  ' contributors.json > docs/contributors.json
}

if generate; then
  echo "Excluded (members/owners):"; cat members.txt
  echo "Thanked (external):"; jq -r '.[].login' docs/contributors.json
else
  echo "::warning::contributor generation failed; baking an empty list"
  echo '[]' > docs/contributors.json
fi
