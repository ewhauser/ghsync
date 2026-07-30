#!/bin/sh

set -eu

events="
pull_request
pull_request_review
pull_request_review_comment
pull_request_review_thread
check_run
check_suite
push
"

repository_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
corpus_root="$repository_root/internal/conformance/corpus"
version_file="$corpus_root/VERSION"
if [ ! -f "$version_file" ]; then
	printf 'missing corpus version pin: %s\n' "$version_file" >&2
	exit 1
fi
if [ "$(sed -n '$=' "$version_file")" != 1 ]; then
	printf 'corpus VERSION must contain exactly one line\n' >&2
	exit 1
fi
release_tag=$(sed -n '1p' "$version_file")
if ! grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+$' "$version_file"; then
	printf 'invalid corpus release tag: %s\n' "$release_tag" >&2
	exit 1
fi

temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/ghsync-webhooks.XXXXXX")
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM

archive="$temporary_root/webhooks.tar.gz"
curl --fail --location --silent --show-error \
	"https://github.com/octokit/webhooks/archive/refs/tags/$release_tag.tar.gz" \
	--output "$archive"
tar -xzf "$archive" -C "$temporary_root"

source_root="$temporary_root/webhooks-${release_tag#v}"
staged_corpus="$temporary_root/corpus"
mkdir -p "$staged_corpus"
cp "$source_root/LICENSE" "$staged_corpus/LICENSE"
cp -R "$source_root/payload-schemas/api.github.com/common" "$staged_corpus/common"

for event in $events; do
	destination="$staged_corpus/$event"
	mkdir -p "$destination"
	cp "$source_root/payload-examples/api.github.com/$event/"*.json "$destination/"
	cp "$source_root/payload-schemas/api.github.com/$event/"*.json "$destination/"
done

printf '%s\n' "$release_tag" >"$staged_corpus/VERSION"
rm -rf "$corpus_root"
mkdir -p "$(dirname -- "$corpus_root")"
mv "$staged_corpus" "$corpus_root"
