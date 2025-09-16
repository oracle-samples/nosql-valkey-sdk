# Python tests for RedisJSON

## Test setup

I used [virtual environment](https://docs.python.org/3/library/venv.html) to
install dependencies and run the tests. To create venv and use it:

```bash
cd ~
python3 -m venv .venv
source ~/.venv/bin/activate
```

(Check if you already have ~/.venv, since this already might be the default,
in which case you only need to active the existing venv, alternatively, you
can create a new venv with path other than .venv)

The main dependency of this test is
[RLTest](https://github.com/RedisLabsModules/RLTest). This also will install
other dependencies like
[python redis module](https://pypi.org/project/redis/).

The install command in RLTest github page did not work for me, so I used
regular pip install.

Note: currently RLTest loads pkg_resources module at runtime.  pkg_resources
is part of "setuptools" package. You have to install that separately.

So, do the following:  
("path/to/redis" below is the path to this repo)

```bash
cd path/to/redis/tests/external/redisjson/pytest
source ~/.venv/bin/activate
pip install setuptools
pip install RLTest
./tests.sh
```

You will see dependencies installed under directory like
*.venv/lib/python3.12/site-packages*.

Note: if you are under VPN and using HTTP proxy, you need to provide it to
"pip install" command. E.g.:  
(proxyHost and proxyPort are host and port of your HTTP proxy)

```bash
pip install setuptools --proxy http://proxyHost:proxyPort
pip install RLTest --proxy http://proxyHost:proxyPort
```

Note: pkg_resources is deprecated, so when running RLTest you will get the
deprecation warning, which can be ignored. Hopefully the future version of
RLTest will not use this package anymore and it will not be required. We will
need to revisit this when that happens or if pkg_resources is removed.

## Tests

There are 2 python test files from redisjson repo that are relevant to us:
test.py and test_multi.py. However, most tests in test_multi.py use JSON path
with recursive descent, which we do not currently support. It would be very
many changes to make test_multi.py to not use recursive descent, so skipping
it for now. Once we support recursive descent we will revisit this and enable
test_multi.py.
