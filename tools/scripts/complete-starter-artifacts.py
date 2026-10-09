#!/usr/bin/env python3
# Copyright 2024-2026 the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import html
import os
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

ROOT = Path(__file__).resolve().parents[2]
NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def main():
    root = ET.parse(ROOT / "pom.xml").getroot()
    version = root.findtext("m:properties/m:revision", namespaces=NS)
    completed = 0
    for node in root.findall("m:modules/m:module", NS):
        directory = ROOT / node.text
        pom = ET.parse(directory / "pom.xml").getroot()
        artifact = pom.findtext("m:artifactId", namespaces=NS)
        if pom.findtext("m:packaging", default="jar", namespaces=NS) == "pom":
            continue
        classifiers = [directory / "target" / f"{artifact}-{version}-{kind}.jar" for kind in ("sources", "javadoc")]
        if all(path.is_file() for path in classifiers):
            continue
        # Dependency-only starters contain build metadata, with no Java implementation to document.
        if not artifact.startswith("argi-starter-") or list((directory / "src/main").rglob("*.java")):
            raise ValueError(f"Missing classifiers for a module with Java sources: {artifact}")
        with ZipFile(directory / "target" / f"{artifact}-{version}.jar") as runtime:
            if any(name.endswith(".class") for name in runtime.namelist()):
                raise ValueError(f"Cannot synthesize source documentation for compiled classes: {artifact}")
        rows = []
        for dependency in pom.findall("m:dependencies/m:dependency", NS):
            group = dependency.findtext("m:groupId", namespaces=NS)
            name = dependency.findtext("m:artifactId", namespaces=NS)
            value = dependency.findtext("m:version", default="Managed by the release parent POM", namespaces=NS)
            value = value.replace("${project.version}", version)
            rows.append(f"<tr><td>{html.escape(group or '')}</td><td>{html.escape(name or '')}</td><td>{html.escape(value)}</td></tr>")
        title = html.escape(f"{artifact} {version}")
        documentation = (
            '<!doctype html><html lang="en"><meta charset="utf-8">'
            f'<title>{title}</title><body><h1>{title}</h1>'
            '<p>This starter declares dependencies and contains no Java source or public classes. '
            'Its runtime behavior is provided by the dependencies below. The source archive contains '
            'the actual module POM and license.</p><table><thead><tr><th>Group</th><th>Artifact</th>'
            '<th>Version</th></tr></thead><tbody>' + ''.join(rows) + '</tbody></table></body></html>'
        )
        for archive_path in classifiers:
            if archive_path.exists():
                continue
            with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as archive:
                archive.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\n\n")
                archive.write(directory / "pom.xml", "pom.xml")
                archive.write(ROOT / "LICENSE", "LICENSE")
                archive.writestr("index.html", documentation)
            signature = archive_path.with_name(archive_path.name + ".asc")
            subprocess.run([
                "gpg", "--batch", "--yes", "--pinentry-mode", "loopback", "--passphrase-fd", "0",
                "--armor", "--detach-sign", "--output", str(signature), str(archive_path),
            ], input=(os.environ.get("MAVEN_GPG_PASSPHRASE", "") + "\n").encode(), check=True)
            completed += 1
        print(f"Completed dependency-only starter classifiers: {artifact}")
    print(f"Created and signed {completed} source/documentation archives")


if __name__ == "__main__":
    main()
