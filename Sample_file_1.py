import pandas as pd

df = pd.DataFrame({
    'X': ['A', 'B', 'A', 'C'],
    'Y': [10, 7, 12, 5]
})

arr = df.values # Noncompliant: using the 'values' attribute is not recommended


class Empty:
pass

class Add:
def __add__(self, other):
    return 42

Empty() + 1  # Noncompliant: no __add__ method is defined on the Empty class
Add() + 1
1 + Add()  # Noncompliant: no __radd__ method is defined on the Add class
Add() + Empty()
Empty() + Add()  # Noncompliant: no __radd__ method is defined on the Add class


try:
...
except OSError as e:
raise RuntimeError("Something went wrong") from e

while node is not None:
node = node.parent()
print(node)
break

from typing import Generic, TypeVar

_T_co = TypeVar("_T_co", covariant=True, bound=str)

class ClassA(Generic[_T_co]):
def method1(self)  _T_co:
    ...

class MyClass:
def __add__(self, other):
    raise NotImplementedError()  # Noncompliant: the exception will be propagated
def __radd__(self, other):
    raise NotImplementedError()  # Noncompliant: the exception will be propagated

class MyOtherClass:
def __add__(self, other):
    return 42
def __radd__(self, other):
    return 42

MyClass() + MyOtherClass()  # This will raise NotImplementedError


a = 1
b = "1"
value = a is b  # Noncompliant. Always False
value = a is not b  # Noncompliant. Always True

import unittest
class MyTest(unittest.TestCase):

def test_something(self):
    if not external_resource_available():
        return  # Noncompliant
    self.assertEqual(foo(), 42)

class Rectangle(object):

@classmethod
def area(bob, height, width):  #Noncompliant
return height * width

from typing import List

def foo(elements: List[str]):
for elt in elements:
    if elt.isnumeric():
        return elt
else:  # Noncompliant: no break in the loop
    raise ValueError("List does not contain any number")

def bar(elements: List[str]):
for elt in elements:
    if elt.isnumeric():
        return elt
else:  # Noncompliant: no break in the loop
    raise ValueError("List does not contain any number")

class ExampleModel(models.Model):
  name = models.CharField(max_length=50, null=True)

######################
# Positional Arguments
######################

param_args = [1, 2, 3]
param_kwargs = {'x': 1, 'y': 2}

def func(a, b=1):
print(a, b)

def positional_unlimited(a, b=1, *args):
print(a, b, *args)

func(1)
func(1, 42)
func(1, 2, 3)  # Noncompliant. Too many positional arguments
func()  # Noncompliant. Missing positional argument for "a"

positional_unlimited(1, 2, 3, 4, 5)

def positional_limited(a, *, b=2):
print(a, b)

positional_limited(1, 2)  # Noncompliant. Too many positional arguments


#############################
# Unexpected Keyword argument
#############################

def keywords(a=1, b=2, *, c=3):
print(a, b, c)

keywords(1)
keywords(1, z=42)  # Noncompliant. Unexpected keyword argument "z"

def keywords_unlimited(a=1, b=2, *, c=3, **kwargs):
print(a, b, kwargs)

keywords_unlimited(a=1, b=2, z=42)

#################################
# Mandatory Keyword argument only
#################################

def mandatory_keyword(a, *, b):
print(a, b)

mandatory_keyword(1, b=2)
mandatory_keyword(1)  # Noncompliant. Missing keyword argument "b"

def func(a, b, c):
return a * b * c

func(6, 93, 31, c=62) # Noncompliant: argument "c" is duplicated

params = {'c':31}
func(6, 93, 31, **params) # Noncompliant: argument "c" is duplicated
func(6, 93, c=62, **params) # Noncompliant: argument "c" is duplicated

class MyContextManager:
def __enter__(self):
    print("Entering")

def __exit__(self, exc_type, exc_val, exc_tb):
    print("Exiting")


with MyContextManager():
print("Executing body")

