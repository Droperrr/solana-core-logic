import ast
import os
from pathlib import Path

ENRICH_DIR = Path(__file__).parent.parent / "enrich_parser" / "enrich"

class ExceptQCVisitor(ast.NodeVisitor):
    def __init__(self):
        self.issues = []
        self.current_file = None

    def visit_ExceptHandler(self, node):
        # Проверяем, есть ли внутри except вызов qc_tags.append
        has_qc_tag = False
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                if (
                    isinstance(child.func, ast.Attribute)
                    and child.func.attr == "append"
                    and isinstance(child.func.value, ast.Name)
                    and child.func.value.id == "qc_tags"
                ):
                    has_qc_tag = True
                    break
        if not has_qc_tag:
            self.issues.append((self.current_file, node.lineno))
        self.generic_visit(node)


def audit_enrich_dir():
    visitor = ExceptQCVisitor()
    for pyfile in ENRICH_DIR.glob("*.py"):
        with open(pyfile, "r", encoding="utf-8") as f:
            source = f.read()
        try:
            tree = ast.parse(source, filename=str(pyfile))
            visitor.current_file = pyfile.name
            visitor.visit(tree)
        except Exception as e:
            print(f"ERROR parsing {pyfile}: {e}")
    return visitor.issues

if __name__ == "__main__":
    issues = audit_enrich_dir()
    if not issues:
        print("Meta-QC: Все except-блоки enrichment-модулей содержат добавление QC-тега.")
    else:
        print("Meta-QC: Найдены except-блоки без добавления QC-тега:")
        for fname, lineno in issues:
            print(f"  - {fname}: строка {lineno}") 