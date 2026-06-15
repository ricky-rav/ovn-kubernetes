"""Macros for the OVN-Kubernetes docs site (mkdocs-macros-plugin).

Provides ``{{ section_index() }}``: auto-generates a section landing page's
list of links from the files present in that section's directory, so a section
index never drifts out of sync with the section's content.

Each listed page may set optional front matter to refine how it appears:

    ---
    description: One-line summary shown next to the link.
    weight: 10          # lower sorts first; unweighted pages sort last
    ---

With no front matter the list falls back to: sub-sections first, then pages,
each ordered alphabetically by title.
"""
import os

import yaml  # PyYAML — already a dependency

# Never list these in an auto-generated index: the index page itself, doc
# templates, and pages that physically live here but are curated elsewhere.
EXCLUDE = {"index.md", "requirements.md"}


def _read_front_matter(path):
    """Return (front_matter_dict, body_lines) for a Markdown file."""
    try:
        with open(path, encoding="utf-8") as fh:
            lines = fh.readlines()
    except OSError:
        return {}, []
    if not lines or lines[0].strip() != "---":
        return {}, lines
    fm, i = [], 1
    while i < len(lines) and lines[i].strip() != "---":
        fm.append(lines[i])
        i += 1
    if i >= len(lines):  # no closing delimiter — treat whole file as body
        return {}, lines
    try:
        meta = yaml.safe_load("".join(fm)) or {}
    except yaml.YAMLError:
        meta = {}
    return (meta if isinstance(meta, dict) else {}), lines[i + 1:]


def _first_h1(body):
    for line in body:
        s = line.strip()
        if s.startswith("# "):
            return s[2:].strip()
    return None


def _entry(path, link, is_page):
    meta, body = _read_front_matter(path)
    title = (
        meta.get("title")
        or _first_h1(body)
        or os.path.splitext(os.path.basename(path))[0].replace("-", " ").replace("_", " ").title()
    )
    try:
        weight = float(meta.get("weight"))
    except (TypeError, ValueError):
        weight = float("inf")
    return {
        "title": title,
        "desc": str(meta.get("description") or "").strip(),
        "link": link,
        "weight": weight,
        "is_page": 1 if is_page else 0,
    }


def define_env(env):
    docs_dir = env.conf["docs_dir"]

    @env.macro
    def section_index():
        """List the current section's sub-sections and pages as Markdown links."""
        rel_dir = os.path.dirname(env.page.file.src_path)  # e.g. "features"
        abs_dir = os.path.join(docs_dir, rel_dir)

        entries = []
        for name in sorted(os.listdir(abs_dir)):
            if name in EXCLUDE or "template" in name.lower():
                continue
            full = os.path.join(abs_dir, name)
            child_index = os.path.join(full, "index.md")
            if os.path.isdir(full) and os.path.isfile(child_index):
                entries.append(_entry(child_index, name + "/", is_page=False))
            elif name.endswith(".md"):
                entries.append(_entry(full, name, is_page=True))

        # Sort by weight, then sub-sections before pages, then title.
        entries.sort(key=lambda e: (e["weight"], e["is_page"], e["title"].lower()))

        lines = []
        for e in entries:
            if e["desc"]:
                lines.append(f"- [{e['title']}]({e['link']}) &mdash; {e['desc']}")
            else:
                lines.append(f"- [{e['title']}]({e['link']})")
        return "\n".join(lines) if lines else "_No pages in this section yet._"
