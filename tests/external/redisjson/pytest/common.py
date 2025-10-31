import signal
from contextlib import contextmanager
from functools import wraps

from RLTest.env import Query

from includes import *
from packaging import version
from unittest import SkipTest
from RLTest import Env
import inspect
import json

@contextmanager
def TimeLimit(timeout):
    def handler(signum, frame):
        raise Exception('TimeLimit timeout')

    signal.signal(signal.SIGALRM, handler)
    signal.setitimer(signal.ITIMER_REAL, timeout, 0)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)

def skipOnExistingEnv(env):
    if 'existing' in env.env:
        env.skip()

def skipOnCrdtEnv(env):
    if len([a for a in env.cmd('module', 'list') if a[1] == 'crdt']) > 0:
        env.skip()

def skip(f, on_cluster=False):
    @wraps(f)
    def wrapper(env, *args, **kwargs):
        if not on_cluster or env.isCluster():
            env.skip()
            return
        return f(env, *args, **kwargs)
    return wrapper

def no_san(f):
    @wraps(f)
    def wrapper(env, *args, **kwargs):
        if SANITIZER != '':
            fname = f.__name__
            env.debugPrint("skipping {} due to sanitizer".format(fname), force=True)
            env.skip()
            return
        return f(env, *args, **kwargs)
    return wrapper

def skip_redis_less_than(redis_less_than=None):
    def decorate(f):
        def wrapper():
            if redis_less_than and server_version_is_less_than(redis_less_than):
                raise SkipTest()
            if len(inspect.signature(f).parameters) > 0:
                env = Env()
                return f(env)
            else:
                return f()
        return wrapper
    return decorate


server_ver = None
def server_version_is_at_least(ver):
    global server_ver
    if server_ver is None:
        import subprocess
        # Expecting something like "Redis server v=7.2.3 sha=******** malloc=jemalloc-5.3.0 bits=64 build=***************"
        v = subprocess.run([Defaults.binary, '--version'], stdout=subprocess.PIPE).stdout.decode().split()[2].split('=')[1]
        server_ver = version.parse(v)
    if not isinstance(ver, version.Version):
        ver = version.parse(ver)
    return server_ver >= ver

def server_version_is_less_than(ver):
    return not server_version_is_at_least(ver)

# Change: these tests often compare returned JSON values. However, in JSON
# the order of object keys is not defined, so the tests give false positive
# when the returned string has object keys in different order. The below is
# a workaround to correct this by sorting the object keys before comparison.
# Regarding the ..._list_... functions below:
# Redis JSON (with non-legacy paths) usually returns an array of results. For
# some cases such as '$.*' where .* refers to object fields, the order of
# results in that array is not defined, so we should compare such arrays
# without regards to order.

def sort_json_val(val):
    if isinstance(val, list):
        return list(map(sort_json_val, val))
    if not isinstance(val, dict):
        return val
    sortedList = sorted(val.items())
    sortedVal = {}
    for k, v in sortedList:
        sortedVal[k] = sort_json_val(v)
    return sortedVal

def sort_json_list_val(val):
    if (isinstance(val, list)):
        val = sorted(val, key=str)
    return val

def sort_json(str):
    return json.dumps(sort_json_val(json.loads(str)))

def sort_json_list(str):
    return json.dumps(sort_json_list_val(json.loads(str)))

def assertJsonEqual(self, first, second, depth=0, message=None):
    if (isinstance(first, str)):
        self.assertEqual(sort_json(first), sort_json(second), depth, message)
    else:
        self.assertEqual(sort_json_val(first), sort_json_val(second), depth, message)

def assertJsonListEqual(self, first, second, depth=0, message=None):
    if (isinstance(first, str)):
        self.assertEqual(sort_json_list(first), sort_json_list(second), depth, message)
    else:
        self.assertEqual(sort_json_list_val(first), sort_json_list_val(second), depth, message)

def equalJson(self, expected):
    self.env.assertJsonEqual(self.res, expected, 1)
    return self

def equalJsonList(self, expected):
    self.env.assertJsonListEqual(self.res, expected, 1)
    return self

Env.assertJsonEqual = assertJsonEqual
Env.assertJsonListEqual = assertJsonListEqual
Query.equalJson = equalJson
Query.equalJsonList = equalJsonList


