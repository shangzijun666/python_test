class Student:
    company = 'stepone'
    count = 0

    def __init__(self,name,age):
        self.name=name
        self.age=age
        Student.count=Student.count+1
    def say_age(self):
        print(f"我的公司是{Student.company}")
        print(f"我的数是 {Student.count}")

if __name__ == '__main__':

    s1 = Student("szj","24")
    s1.say_age()
    print(f"一共创建了{Student.count}个对象")