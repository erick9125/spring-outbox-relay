# Releasing

Publishing goes through the [Central Portal](https://central.sonatype.com). Sonatype has no official
Gradle plugin for it, so the build produces the deployment bundle the Portal's API expects and CI
uploads it with `curl`. No third-party publishing plugin is involved.

The pieces below marked **once** are identity and credentials: they cannot be automated from this
repository and have to be done by whoever owns the namespace.

---

## 1. Claim the namespace — once

Nothing else works until `io.github.erick9125` is verified. At
[central.sonatype.com](https://central.sonatype.com) → *Namespaces* → *Add Namespace*, add
`io.github.erick9125`. The Portal responds with a generated repository name; create a public GitHub
repository with exactly that name under the `erick9125` account, then press *Verify*. The repository
can be deleted afterwards.

## 2. Create a signing key — once

Maven Central requires every artifact to be signed, and the public key must be discoverable on a
public keyserver.

```bash
gpg --full-generate-key                      # RSA 4096, no expiry is fine
gpg --list-secret-keys --keyid-format=long    # note the key id
gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID>
gpg --armor --export-secret-keys <KEY_ID>     # this whole block is SIGNING_KEY
```

Keep the private key out of the repository. It only ever travels as a secret.

## 3. Create a Portal token — once

At [central.sonatype.com](https://central.sonatype.com) → *View Account* → *Generate User Token*.
The Portal returns a username and a password; both are needed.

## 4. Add the repository secrets — once

`Settings → Secrets and variables → Actions`:

| Secret | Value |
| --- | --- |
| `SIGNING_KEY` | the full ASCII-armoured private key from step 2, `-----BEGIN` line included |
| `SIGNING_PASSWORD` | the passphrase protecting that key |
| `CENTRAL_TOKEN_USERNAME` | token username from step 3 |
| `CENTRAL_TOKEN_PASSWORD` | token password from step 3 |

---

## Cutting a release

The version comes from the tag, so there is nothing to edit in the build:

```bash
git tag v0.1.0
git push origin v0.1.0
```

`.github/workflows/release.yml` then runs `check`, builds the bundle, verifies it is signed, and
uploads it. The upload is `USER_MANAGED`: the deployment appears at
[central.sonatype.com/publishing/deployments](https://central.sonatype.com/publishing/deployments)
and **nothing reaches Maven Central until you publish it there**. Review it first — a released
version can never be replaced or removed.

## Building a bundle locally

```bash
./gradlew centralBundle -PoutboxVersion=0.1.0 \
  -PsigningKey="$(gpg --armor --export-secret-keys <KEY_ID>)" \
  -PsigningPassword=...
```

The bundle lands in `build/central/`. Without a version override the build is a SNAPSHOT and is
built unsigned, which is why `check` needs no key. Asking for a release version without a key fails
on purpose rather than producing a bundle Central would reject after the upload.

Inspecting what would be published:

```bash
unzip -l build/central/central-bundle-0.1.0.zip
```

Expect the jar, sources jar, javadoc jar, POM and Gradle module metadata — five artifacts — each
with `.asc`, `.md5`, `.sha1`, `.sha256` and `.sha512` alongside, and nothing else. The staging
directory is wiped before each publish: it is a plain Maven repository, so without that a release
bundle would carry whatever earlier builds left in it, unsigned SNAPSHOT artifacts included.

To check the signatures rather than just their presence:

```bash
cd $(mktemp -d) && unzip -q .../central-bundle-0.1.0.zip
cd io/github/erick9125/spring-outbox-relay/0.1.0
for f in *.jar *.pom *.module; do gpg --verify "$f.asc" "$f"; done
```

## Why the build looks the way it does

Two things in `build.gradle.kts` exist because of bugs that shipped unnoticed:

- **`versionMapping`** on the publication. Dependencies are declared without versions because the
  Spring Boot BOM supplies them at resolution time, and that writes nothing into the POM. Publishing
  failed outright, and forcing it would have handed consumers a POM they could not resolve.
  `verifyPublishedPom` runs in `check` so this cannot regress.
- **`generatedPomCustomization { enabled(false) }`**. The dependency-management plugin otherwise
  copies `spring-boot-dependencies` into the published POM, nudging a consuming build's Spring Boot
  versions towards ours.
