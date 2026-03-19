import sys
from proposed_skill import *

import pytest
from calculator import multiply, add

def test_multiply():
    assert multiply(a=5.0, b=2.0) == 10.0
    assert multiply(a=-3.0, b=4.0) == -12.0
    assert multiply(a=0.0, b=10.0) == 0.0

def test_add():
    assert add(a=5.0, b=2.0) == 7.0
    assert add(a=-3.0, b=4.0) == 1.0
    assert add(a=0.0, b=10.0) == 10.0
