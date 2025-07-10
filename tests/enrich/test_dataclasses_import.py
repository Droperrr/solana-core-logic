print('DEBUG: test_dataclasses_import started')
try:
    from dataclasses import dataclass, field
    print('DEBUG: dataclasses import OK')
except Exception as e:
    print('DEBUG: dataclasses import FAIL:', e) 