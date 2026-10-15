from pathlib import Path
import hashlib,json,sys
p=Path(__file__).resolve().parent
m=json.loads((p/'PROTECTED_SHA256.json').read_text())
bad=[f for f,h in m.items() if not (p/f).is_file() or (p/f).is_symlink() or hashlib.sha256((p/f).read_bytes()).hexdigest()!=h]
print('PROTECTED_PASS' if not bad else 'PROTECTED_FAIL '+repr(bad))
sys.exit(bool(bad))
