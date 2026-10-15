import sys,json,unittest,tempfile,shutil,hashlib
from pathlib import Path
P=Path(__file__).resolve().parents[1];sys.path.insert(0,str(P/'src'))
from first_migration_plan import build_migration_plan
H=json.loads((P/'fixtures/expected_migrations.json').read_text())
class Public(unittest.TestCase):
 def test_exact_deterministic_immutable(self):
  a=build_migration_plan(P/'reference')
  expected=tuple((f,h,(P/'reference'/f).read_bytes().decode('utf-8')) for f,h in H.items())
  self.assertEqual(a.entries,expected);self.assertIs(type(a.entries),tuple);self.assertTrue(all(type(x) is tuple for x in a.entries));self.assertEqual(a,build_migration_plan(P/'reference'));self.assertEqual(a.status,'PLANNED_NOT_AUTHORIZED');self.assertEqual(a.required_preflight,tuple(json.loads((P/'CONTRACT.json').read_text())['required_preflight']))
 def test_guards(self):
  for schema in ['other',None,True]:
   with self.assertRaisesRegex(ValueError,'^SCHEMA_UNSUPPORTED$'):build_migration_plan(P/'reference',schema=schema)
 def test_missing_changed_and_symlink(self):
  for mode,code in [('missing','MISSING_MIGRATION'),('changed','HASH_MISMATCH'),('symlink','UNSAFE_PATH')]:
   with self.subTest(mode=mode),tempfile.TemporaryDirectory() as d:
    root=Path(d)/'r';shutil.copytree(P/'reference',root);f=root/list(H)[-1]
    if mode=='missing':f.unlink()
    elif mode=='changed':f.write_bytes(f.read_bytes()+b'\n')
    else:f.unlink();f.symlink_to((P/'reference'/list(H)[-1]).resolve())
    with self.assertRaisesRegex(ValueError,'^'+code+'$'):build_migration_plan(root)
 def test_traversal(self):
  with self.assertRaisesRegex(ValueError,'^UNSAFE_PATH$'):build_migration_plan(P/'reference'/'..'/'reference')

 def test_invalid_root(self):
  for root in [None, 42, P/'missing-root', P/'CONTRACT.json']:
   with self.subTest(root=root), self.assertRaisesRegex(ValueError,'^INVALID_ROOT$'):build_migration_plan(root)
 def test_schema_precedes_root(self):
  with self.assertRaisesRegex(ValueError,'^SCHEMA_UNSUPPORTED$'):build_migration_plan(None,schema='other')
