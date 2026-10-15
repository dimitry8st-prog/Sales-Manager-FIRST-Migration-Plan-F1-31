"""Дополнительные проверки реализации F1-31."""

import hashlib
import inspect
import os
import shutil
import sys
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch


PACKAGE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE_ROOT / "src"))

from first_migration_plan import build_migration_plan


class CandidatePlanTests(unittest.TestCase):
    def test_schema_checked_before_filesystem(self):
        # Неверная schema отклоняется даже при плохом root.
        for schema in ("public", "", None, True, 42):
            with self.subTest(schema=schema):
                with self.assertRaisesRegex(
                    ValueError, "^SCHEMA_UNSUPPORTED$"
                ):
                    # Намеренно передаём неверные типы.
                    build_migration_plan(
                        None,
                        schema=schema,  # pyright: ignore[reportArgumentType]
                    )

    def test_string_root_supported(self):
        # Контракт разрешает и Path, и обычную строку.
        root = PACKAGE_ROOT / "reference"

        self.assertEqual(
            build_migration_plan(root),
            build_migration_plan(str(root)),
        )

    def test_source_files_unchanged(self):
        root = PACKAGE_ROOT / "reference"
        files = sorted(root.rglob("*.sql"))

        before = {
            path.relative_to(root): path.read_bytes()
            for path in files
        }

        plan = build_migration_plan(root)

        after = {
            path.relative_to(root): path.read_bytes()
            for path in files
        }

        self.assertEqual(before, after)

        # SQL в результате восстанавливает исходные байты.
        for relative_path, expected_hash, raw_sql in plan.entries:
            with self.subTest(path=relative_path):
                raw_bytes = raw_sql.encode("utf-8")

                self.assertEqual(
                    raw_bytes,
                    before[Path(relative_path)],
                )
                self.assertEqual(
                    hashlib.sha256(raw_bytes).hexdigest(),
                    expected_hash,
                )

    def test_each_changed_migration_rejected(self):
        reference = PACKAGE_ROOT / "reference"
        plan = build_migration_plan(reference)

        for relative_path, _, _ in plan.entries:
            with self.subTest(path=relative_path):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory) / "reference"
                    shutil.copytree(reference, root)

                    path = root / relative_path
                    path.write_bytes(path.read_bytes() + b"\n")

                    # Изменение любого SQL отменяет выдачу плана.
                    with self.assertRaisesRegex(
                        ValueError, "^HASH_MISMATCH$"
                    ):
                        build_migration_plan(root)

    def test_directory_instead_of_sql_rejected(self):
        reference = PACKAGE_ROOT / "reference"
        relative_path = build_migration_plan(
            reference
        ).entries[0][0]

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "reference"
            shutil.copytree(reference, root)

            path = root / relative_path
            path.unlink()
            path.mkdir()

            with self.assertRaisesRegex(
                ValueError, "^MISSING_MIGRATION$"
            ):
                build_migration_plan(root)

    def test_symlink_root_rejected(self):
        # Даже ссылка на правильную папку запрещена контрактом.
        reference = PACKAGE_ROOT / "reference"

        with tempfile.TemporaryDirectory() as directory:
            link = Path(directory) / "linked_root"
            link.symlink_to(
                reference,
                target_is_directory=True,
            )

            with self.assertRaisesRegex(
                ValueError, "^UNSAFE_PATH$"
            ):
                build_migration_plan(link)

    def test_symlink_parent_directory_rejected(self):
        # Проверяем ссылку среди каталогов внутри root.
        reference = PACKAGE_ROOT / "reference"

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "reference"
            shutil.copytree(reference, root)

            original = root / "migrations"
            relocated = Path(directory) / "relocated_migrations"
            original.rename(relocated)
            original.symlink_to(
                relocated,
                target_is_directory=True,
            )

            with self.assertRaisesRegex(
                ValueError, "^UNSAFE_PATH$"
            ):
                build_migration_plan(root)

    def test_module_load_has_no_filesystem_io(self):
        # Чтение исходника — подготовка теста, до запрета I/O.
        source = (
            PACKAGE_ROOT / "src" / "first_migration_plan.py"
        ).read_text(encoding="utf-8")

        code = compile(
            source,
            "first_migration_plan.py",
            "exec",
        )

        def forbidden(*args, **kwargs):
            raise AssertionError(
                "Filesystem I/O during module initialization"
            )

        # Проверяем, что сам запрет действительно работает.
        with patch("builtins.open", side_effect=forbidden):
            with self.assertRaisesRegex(
                AssertionError,
                "^Filesystem I/O during module initialization$",
            ):
                open("candidate_probe_must_not_be_opened")

        # Этот блок внутри теста, но СНАРУЖИ forbidden.
        with ExitStack() as stack:
            stack.enter_context(
                patch("builtins.open", side_effect=forbidden)
            )

            for method in (
                "open",
                "read_bytes",
                "read_text",
                "stat",
                "lstat",
                "resolve",
                "absolute",
                "exists",
                "is_file",
                "is_dir",
                "is_symlink",
                "iterdir",
                "glob",
                "rglob",
                "cwd",
            ):
                stack.enter_context(
                    patch.object(
                        Path,
                        method,
                        side_effect=forbidden,
                    )
                )

            for method in (
                "open",
                "stat",
                "lstat",
                "listdir",
                "scandir",
                "getcwd",
            ):
                stack.enter_context(
                    patch.object(
                        os,
                        method,
                        side_effect=forbidden,
                    )
                )

            namespace = {
                "__name__": "candidate_import_check",
            }

            # Выполняем код модуля, а не только компилируем его.
            exec(code, namespace)

        # Подтверждаем, что выполнение дошло до создания функции.
        self.assertIn("MigrationPlan", namespace)
        self.assertTrue(
            callable(namespace["build_migration_plan"])
        )

    def test_exact_function_signature(self):
        signature = inspect.signature(build_migration_plan)
        parameters = signature.parameters

        # Никаких executor, manifest или foundation_ready.
        self.assertEqual(
            tuple(parameters),
            ("root", "schema"),
        )
        self.assertEqual(
            parameters["root"].kind,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
        self.assertIs(
            parameters["root"].default,
            inspect.Parameter.empty,
        )
        self.assertEqual(
            parameters["schema"].kind,
            inspect.Parameter.KEYWORD_ONLY,
        )
        self.assertEqual(
            parameters["schema"].default,
            "runtime_next",
        )