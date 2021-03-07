# 显示系统欢迎信息
print('-' * 20,'欢迎使用员工管理系统','-' * 20)

emps = ['孙悟空\t10000\t男\t花果山','猪八戒\t9000\t男\t高老庄']


while True :
  print('请选择要做的操作 : ')
  print('\t0 退出系统')
  print('\t1 查询员工')
  print('\t2 添加员工')
  print('\t3 删除员工')

  user_choose = input("请选择[0 - 3] : ")

  print('-' * 62)

  if user_choose == '0':
    print('你退出了管理系统...')
    break
  elif user_choose == '1':
    print('\t序号\t姓名\t年龄\t性别\t住址')

    n = 1

    for emp in emps:
      print(f'\t{n}\t{emp}')
      n += 1
  elif user_choose == '2':
    pass
  elif user_choose == '3':
    pass
  else :
    print('输入错误，请重新输入！')

  print('-' * 62)

print(user_choose)
