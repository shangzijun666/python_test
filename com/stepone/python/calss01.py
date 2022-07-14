# class Student:
#     def __init__(self,name,age):
#         self.name=name
#         self.age=age
#
#     def say_age(self):
#         print(f"我的名字叫{self.name} 年龄 是{self.age}")
#
# if __name__ == '__main__':
#
#     s1 = Student("尚子钧","24")
#     print(s1.age)
#     print(s1.name)
#     s1.say_age()
#     print(dir(s1))
#     print(s1.__dict__)
#     print(isinstance(s1,Student))
class Student:
    pass
stu2 = Student
s1 = stu2
if __name__ == '__main__':

    print(type(Student))
    print(id(Student))
    print(s1)