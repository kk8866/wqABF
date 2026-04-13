import inspect
# print(inspect.stack())
def xxx():
    pass
def ccc():
    pass
# frame = inspect.currentframe()
# class_name = frame.f_back.f_locals.get(__file__)
import temp
print(inspect.getmembers(temp, inspect.isfunction))