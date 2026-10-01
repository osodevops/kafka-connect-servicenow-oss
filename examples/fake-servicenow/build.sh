#!/usr/bin/env bash
# Populates examples/fake-servicenow/lib/ with everything the fake ServiceNow needs, so the
# Dockerfile can be built without Maven: the snow-core jar, its test-jar (which contains the
# fake, sh.oso.servicenow.testing.*) and the full test-scope dependency closure.
#
#   ./fake-servicenow/build.sh && docker compose --profile fake up -d
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
lib="$here/lib"

if ! compgen -G "$repo/snow-core/target/snow-core-*-tests.jar" >/dev/null; then
  echo "snow-core jars not found; building snow-core (tests skipped)..."
  (cd "$repo" && mvn -q -pl snow-core -am package -DskipTests -Dspotless.check.skip=true -Djacoco.skip=true)
fi

rm -rf "$lib"
mkdir -p "$lib"

echo "copying snow-core test-scope dependencies to $lib"
(cd "$repo" && mvn -q -pl snow-core dependency:copy-dependencies \
  -DincludeScope=test -DoutputDirectory="$lib")

shopt -s nullglob
jars=("$repo"/snow-core/target/snow-core-*.jar)
if [ "${#jars[@]}" -eq 0 ]; then
  echo "no snow-core jars in $repo/snow-core/target after the build" >&2
  exit 1
fi
for jar in "${jars[@]}"; do
  case "$jar" in
    *-sources.jar|*-javadoc.jar) ;;
    *) cp "$jar" "$lib/"; echo "copied $(basename "$jar")" ;;
  esac
done

echo "done: $(ls "$lib" | wc -l | tr -d ' ') jars in $lib"
echo "next: docker compose --profile fake up -d"
