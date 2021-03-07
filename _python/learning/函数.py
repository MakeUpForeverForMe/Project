# def sumNum(*nums) :
#   a = []
#   for num in nums :
#     a.append(num)
#   return a

# r = sumNum(10,20,30)

# print(r)

# help([])

# m = 10
# print('全局测试 m 地址 m =',id(m))
# m = 20
# print('全局测试 m 地址 m =',id(m))


def a():
  global m
  m = 10
  print('内层--1--1 a() m =',id(m),'m =',m)
  def b():
    # print('内层--2--1 b() m =',id(m),'m =',m) # UnboundLocalError: local variable 'm' referenced before assignment
    # global m # 不起作用！因为global是全局，而非半全局变量
    m = 20
    print('内层--2--2 b() m =',id(m),'m =',m)
  b()
  print('内层--1--2 a() m =',id(m),'m =',m)

a()
