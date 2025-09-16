
import sys
import os
from RLTest import Defaults

Defaults.decode_responses = True

ONLY_STABLE = os.getenv('ONLY_STABLE', '0') == '1'
SANITIZER = os.getenv('SANITIZER', '')
VALGRIND = os.getenv('VALGRIND', '0') == '1'
CODE_COVERAGE = os.getenv('CODE_COVERAGE', '0') == '1'
