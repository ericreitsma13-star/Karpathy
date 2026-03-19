import sys
from proposed_skill import *

import pytest

def test_multiply_positive_numbers():
    assert multiply(2.0, 3.0) == 6.0
    assert multiply(10.0, 0.5) == 5.0
    assert multiply(1.0, 1.0) == 1.0

def test_multiply_negative_numbers():
    assert multiply(-2.0, 3.0) == -6.0
    assert multiply(2.0, -3.0) == -6.0
    assert multiply(-2.0, -3.0) == 6.0

def test_multiply_with_zero():
    assert multiply(5.0, 0.0) == 0.0
    assert multiply(0.0, 100.0) == 0.0
    assert multiply(0.0, 0.0) == 0.0

def test_add_positive_numbers():
    assert add(5.0, 7.0) == 12.0
    assert add(1.5, 2.5) == 4.0
    assert add(0.1, 0.2) == pytest.approx(0.3) # Handle potential float precision

def test_add_negative_numbers():
    assert add(-5.0, 7.0) == 2.0
    assert add(5.0, -7.0) == -2.0
    assert add(-5.0, -7.0) == -12.0

def test_add_with_zero():
    assert add(0.0, 10.0) == 10.0
    assert add(5.0, 0.0) == 5.0
    assert add(0.0, 0.0) == 0.0

def test_add_mixed_precision():
    assert add(10, 0.5) == 10.5
    assert add(0.5, 10) == 10.5
