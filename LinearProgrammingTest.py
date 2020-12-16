from scipy import optimize as opt
import numpy as np

c = np.array([2, 3, -5])
A_ub = np.array([[-2,5,-1],[1,3,1]])
B_ub = np.array([-10,12])
A_eq = np.array([[1,1,1]])
B_eq = np.array([7])
x1 = (0,7)
x2 = (0,7)
x3 = (0,7)
res = opt.linprog(
	c=-c,   # objective function: default minimize, add - to maximize
	A_ub=A_ub, # upper bound inequality: [[left_side_1], ..., [left_side_n]]
	b_ub=B_ub, # upper bound inequality: [[rightside_1], ..., [rightside_n]]
	A_eq=A_eq, # equality constraints: [[left_side_1], ..., [left_side_n]]
	b_eq=B_eq, # equality constraints: [[rightside_1], ..., [rightside_n]]
	bounds=(x1, x2, x3) # range of variables
)
print(res)

 #     con: array([1.19830332e-08])
 #     fun: -14.571428542312146
 # message: 'Optimization terminated successfully.'
 #     nit: 5
 #   slack: array([-3.70231614e-08,  3.85714287e+00])
 #  status: 0
 # success: True
 #       x: array([6.42857141e+00, 5.71428573e-01, 9.82192085e-10])